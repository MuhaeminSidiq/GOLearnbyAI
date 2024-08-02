package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xuri/excelize/v2"
)

var (
	totalFiles     int
	processedFiles int
	mu             sync.Mutex
	wg             sync.WaitGroup
)

const dbConfigPath = "db.cfg"

func logError(err error, message string) {
	fmt.Printf("%s: %v\n", message, err)

	currentDir, _ := os.Getwd()
	logDir := filepath.Join(currentDir, "log")
	errorLogFile := filepath.Join(logDir, "error.log")

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}

	file, errFile := os.OpenFile(errorLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if errFile != nil {
		fmt.Printf("Error membuka atau membuat file error log: %v\n", errFile)
		return
	}
	defer file.Close()

	logEntry := fmt.Sprintf("%s: %s: %v\n", time.Now().Format(time.RFC3339), message, err)
	if _, err := file.WriteString(logEntry); err != nil {
		fmt.Printf("Error menulis ke file error log: %v\n", err)
	}
}

func sanitizeFileName(fileName string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return re.ReplaceAllString(fileName, "")
}

func sanitizeColumnName(columnName string) string {
	re := regexp.MustCompile(`[^a-zA-Z0-9]+`)
	return re.ReplaceAllString(strings.ToLower(columnName), "")
}

func determineColumnType(data []string) string {
	isInt := true
	isFloat := true
	isDate := true
	isDatetime := true
	isTimestamp := true
	isTime := true
	isYear := true
	isJSON := true
	isUUID := true
	isBoolean := true
	maxLength := 0

	dateRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	datetimeRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	timestampRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`)
	timeRegex := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	yearRegex := regexp.MustCompile(`^\d{4}$`)
	jsonRegex := regexp.MustCompile(`^\{.*\}$`)
	uuidRegex := regexp.MustCompile(`^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$`)

	for _, value := range data {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if len(value) > maxLength {
			maxLength = len(value)
		}
		if _, err := strconv.Atoi(value); err != nil {
			isInt = false
		} else if len(value) > 10 || (len(value) == 10 && value > "2147483647") {
			// If number length is greater than 10 or equals 10 and greater than max int32 value
			return "BIGINT"
		}
		if _, err := strconv.ParseFloat(value, 64); err != nil {
			isFloat = false
		}
		if !dateRegex.MatchString(value) {
			isDate = false
		}
		if !datetimeRegex.MatchString(value) {
			isDatetime = false
		}
		if !timestampRegex.MatchString(value) {
			isTimestamp = false
		}
		if !timeRegex.MatchString(value) {
			isTime = false
		}
		if !yearRegex.MatchString(value) {
			isYear = false
		}
		if !jsonRegex.MatchString(value) {
			isJSON = false
		}
		if !uuidRegex.MatchString(value) {
			isUUID = false
		}
		if value != "true" && value != "false" && value != "1" && value != "0" {
			isBoolean = false
		}
	}

	switch {
	case isBoolean:
		return "BOOLEAN"
	case isInt:
		return "INT"
	case isFloat:
		if maxLength <= 7 {
			return "FLOAT"
		}
		return "DOUBLE"
	case isDate:
		return "DATE"
	case isDatetime:
		return "DATETIME"
	case isTimestamp:
		return "TIMESTAMP"
	case isTime:
		return "TIME"
	case isYear:
		return "YEAR"
	case isJSON:
		return "JSON"
	case isUUID:
		return "UUID"
	case maxLength <= 255:
		return fmt.Sprintf("VARCHAR(%d)", maxLength)
	case maxLength <= 65535:
		return "TEXT"
	case maxLength <= 16777215:
		return "MEDIUMTEXT"
	default:
		return "LONGTEXT"
	}
}

func escapeString(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "'", "\\'")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func processFile(path string, sem chan struct{}, sqlDir, sqlDataDir string) {
	defer wg.Done()
	defer func() { <-sem }()

	startTime := time.Now()

	xlsx, err := excelize.OpenFile(path)
	if err != nil {
		logError(err, fmt.Sprintf("Error membaca file %s", path))
		logProcessing(path, "error", time.Since(startTime))
		return
	}

	sheetName := xlsx.GetSheetName(xlsx.GetActiveSheetIndex())
	rows, err := xlsx.GetRows(sheetName)
	if err != nil {
		logError(err, fmt.Sprintf("Error mendapatkan baris pada sheet %s", sheetName))
		logProcessing(path, "error", time.Since(startTime))
		return
	}

	if len(rows) > 1 {
		firstRow := rows[0]
		dataRows := rows[1:]
		tableName := sanitizeFileName(strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
		idColumn := fmt.Sprintf("%s_id INT NOT NULL AUTO_INCREMENT COMMENT 'row ID',\n", tableName)
		columnDefinitions := idColumn
		var buffer strings.Builder
		var dataBuffer strings.Builder

		buffer.WriteString(fmt.Sprintf("CREATE TABLE %s (\n%s", tableName, columnDefinitions))

		columnTypes := make([]string, len(firstRow))
		for i, colCell := range firstRow {
			if i > 0 {
				buffer.WriteString(",\n")
			}
			columnData := make([]string, len(dataRows))
			for j, row := range dataRows {
				if i < len(row) {
					columnData[j] = row[i]
				} else {
					columnData[j] = ""
				}
			}
			columnType := determineColumnType(columnData)
			columnTypes[i] = columnType
			sanitizedColumn := sanitizeColumnName(colCell)
			buffer.WriteString(fmt.Sprintf("%s %s DEFAULT NULL COMMENT '%s'", sanitizedColumn, columnType, colCell))
		}

		//buffer.WriteString(fmt.Sprintf(",\nPRIMARY KEY (%s_id)\n) ENGINE = INNODB;", tableName))
		// Menambahkan Primary Key
		buffer.WriteString(fmt.Sprintf(",\nPRIMARY KEY (%s_id)", tableName))

		// Contoh menambahkan indeks untuk kolom yang sering digunakan dalam WHERE atau JOIN
		// Misalnya kita anggap kolom pertama (selain id) sering digunakan dalam WHERE atau JOIN
		if len(firstRow) > 1 {
			firstDataColumn := sanitizeColumnName(firstRow[0])
			buffer.WriteString(fmt.Sprintf(",\nINDEX idx_%s (%s)", firstDataColumn, firstDataColumn))
		}

		buffer.WriteString(fmt.Sprintf("\n) ENGINE = INNODB;"))

		createTableStatement := buffer.String()

		duration := time.Since(startTime)

		sqlFile := filepath.Join(sqlDir, fmt.Sprintf("%s.sql", tableName))
		file, err := os.Create(sqlFile)
		if err != nil {
			logError(err, fmt.Sprintf("Error membuat file SQL untuk %s", path))
			logProcessing(path, "error", duration)
			return
		}
		defer file.Close()

		_, err = file.WriteString(createTableStatement)
		if err != nil {
			logError(err, fmt.Sprintf("Error menulis ke file SQL untuk %s", path))
			logProcessing(path, "error", duration)
			return
		}

		dataFile := filepath.Join(sqlDataDir, fmt.Sprintf("data_%s.sql", tableName))
		data, err := os.Create(dataFile)
		if err != nil {
			logError(err, fmt.Sprintf("Error membuat file data SQL untuk %s", path))
			logProcessing(path, "error", duration)
			return
		}
		defer data.Close()

		for i, row := range dataRows {
			if i%1000000 == 0 {
				if i > 0 {
					dataBuffer.WriteString(";\n")
				}
				dataBuffer.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))
				for j, colCell := range firstRow {
					if j > 0 {
						dataBuffer.WriteString(", ")
					}
					dataBuffer.WriteString(sanitizeColumnName(colCell))
				}
				dataBuffer.WriteString(") VALUES\n")
			} else {
				dataBuffer.WriteString(",\n")
			}

			dataBuffer.WriteString("(")
			for j := range firstRow {
				if j > 0 {
					dataBuffer.WriteString(", ")
				}
				if j < len(row) {
					cell := row[j]
					columnType := columnTypes[j]
					sanitizedValue := escapeString(cell)

					// Handling NULL values and data type constraints
					if sanitizedValue == "" {
						dataBuffer.WriteString("NULL")
					} else {
						switch columnType {
						case "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL", "BOOLEAN":
							dataBuffer.WriteString(sanitizedValue)
						case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
							if isValidDateTime(sanitizedValue, columnType) {
								dataBuffer.WriteString(fmt.Sprintf("'%s'", sanitizedValue))
							} else {
								dataBuffer.WriteString("NULL")
							}
						default:
							dataBuffer.WriteString(fmt.Sprintf("'%s'", sanitizedValue))
						}
					}
				} else {
					dataBuffer.WriteString("NULL")
				}
			}
			dataBuffer.WriteString(")")
		}
		dataBuffer.WriteString(";")

		_, err = data.WriteString(dataBuffer.String())
		if err != nil {
			logError(err, fmt.Sprintf("Error menulis data ke file SQL untuk %s", path))
			logProcessing(path, "error", duration)
			return
		}

		logProcessing(path, "success", duration)
	} else {
		logProcessing(path, "empty", time.Since(startTime))
	}
}

func isValidDateTime(value string, columnType string) bool {
	dateRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	datetimeRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)
	timestampRegex := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$`)
	timeRegex := regexp.MustCompile(`^\d{2}:\d{2}:\d{2}$`)
	yearRegex := regexp.MustCompile(`^\d{4}$`)

	switch columnType {
	case "DATE":
		return dateRegex.MatchString(value)
	case "DATETIME":
		return datetimeRegex.MatchString(value)
	case "TIMESTAMP":
		return timestampRegex.MatchString(value)
	case "TIME":
		return timeRegex.MatchString(value)
	case "YEAR":
		return yearRegex.MatchString(value)
	default:
		return false
	}
}

func logProcessing(filePath, status string, duration time.Duration) {
	mu.Lock()
	defer mu.Unlock()

	currentDir, _ := os.Getwd()
	logDir := filepath.Join(currentDir, "log")
	readLogFile := filepath.Join(logDir, "read.log")

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}

	file, err := os.OpenFile(readLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error membuka atau membuat file read log: %v\n", err)
		return
	}
	defer file.Close()

	processedFiles++
	percentage := float64(processedFiles) / float64(totalFiles) * 100
	logEntry := fmt.Sprintf("%s: %s - %v - %s - %.2f%% selesai\n", time.Now().Format(time.RFC3339), filePath, duration, status, percentage)
	if _, err := file.WriteString(logEntry); err != nil {
		fmt.Printf("Error menulis ke file read.log: %v\n", err)
	}

	fmt.Printf("File: %s, Status: %s, Durasi: %v, %.2f%% selesai\n", filePath, status, duration, percentage)
}

func logRun(status string) {
	mu.Lock()
	defer mu.Unlock()

	currentDir, _ := os.Getwd()
	logDir := filepath.Join(currentDir, "log")
	readRunLogFile := filepath.Join(logDir, "run.log")

	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		os.Mkdir(logDir, 0755)
	}

	file, err := os.OpenFile(readRunLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error membuka atau membuat file read log: %v\n", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error menutup file read log: %v\n", err)
		}
	}(file)

	logEntry := fmt.Sprintf("%s: %s\n", time.Now().Format(time.RFC3339), status)
	if _, err := file.WriteString(logEntry); err != nil {
		fmt.Printf("Error menulis ke file run.log: %v\n", err)
	}
}

func createDefaultDBConfig() error {
	file, err := os.Create(dbConfigPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString("username\npassword\ndatabase\nhostname\nport\n")
	return err
}

func readDBConfig(path string) (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	config := make(map[string]string)
	keys := []string{"username", "password", "database", "hostname", "port"}
	i := 0

	for scanner.Scan() {
		config[keys[i]] = scanner.Text()
		i++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return config, nil
}

func createDBConnection(config map[string]string) (*sql.DB, error) {
	cfg := mysql.Config{
		User:                 config["username"],
		Passwd:               config["password"],
		Net:                  "tcp", // default to tcp, can be changed based on OS
		Addr:                 config["hostname"] + ":" + config["port"],
		DBName:               config["database"],
		AllowNativePasswords: true,
	}

	// Check for Unix socket
	if runtime.GOOS == "linux" {
		if _, err := os.Stat("/var/run/mysqld/mysqld.sock"); err == nil {
			cfg.Net = "unix"
			cfg.Addr = "/var/run/mysqld/mysqld.sock"
		}
	}

	dsn := cfg.FormatDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func executeSQLTableFile(db *sql.DB, path string) error {
	msg1 := fmt.Sprintf("Mulai memproses file %s", path)
	logRun(msg1)
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	statements := strings.Split(string(content), ";")
	for _, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt != "" {
			_, err := db.Exec(trimmedStmt)
			if err != nil {
				return err
			}
		}
	}
	msg2 := fmt.Sprintf("Selesai memproses file %s", path)
	logRun(msg2)
	return nil
}

func processSQLTableFiles(db *sql.DB, dir string) {
	files, err := os.ReadDir(dir)
	if err != nil {
		logError(err, "Gagal membaca file SQL pembuatan tabel pada direktori")
		return
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sql" {
			start := time.Now()
			sqlFilePath := filepath.Join(dir, file.Name())
			err := executeSQLTableFile(db, sqlFilePath)
			duration := time.Since(start)

			if err != nil {
				errMsg := fmt.Sprintf("Error executing %s: %v", file.Name(), err)
				logError(err, errMsg)
			}

			fmt.Printf("Executed %s in %s\n", file.Name(), duration)
		}
	}
}

func processSQLDataFiles(db *sql.DB) {
	// Membaca semua file di direktori SQLData
	files, err := os.ReadDir("SQLData")
	if err != nil {
		errMsg := fmt.Sprintf("Gagal membaca direktori SQLData: %v", err)
		logError(err, errMsg)
		log.Fatalf("Gagal membaca direktori SQLData: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		// Membaca konten file SQL
		filePath := filepath.Join("SQLData", file.Name())
		sqlContent, err := os.ReadFile(filePath)
		if err != nil {
			errMsg := fmt.Sprintf("Gagal membaca file %s: %v", file.Name(), err)
			logError(err, errMsg)
			log.Printf(errMsg)
			continue
		}

		// Mengeksekusi konten file SQL
		start := time.Now()
		if _, err = db.Exec(string(sqlContent)); err != nil {
			errMsg := fmt.Sprintf("Gagal mengeksekusi file %s: %v", file.Name(), err)
			logError(err, errMsg)
			log.Printf(errMsg)
			continue
		}
		duration := time.Since(start)
		rMsg := fmt.Sprintf("Sukses mengeksekusi file %s dalam waktu %s", file.Name(), duration)
		logRun(rMsg)
		log.Printf(rMsg)
	}
}

func main() {
	logRun("Program mulai bekerja.")
	runtime.GOMAXPROCS(runtime.NumCPU())

	currentDir, _ := os.Getwd()
	excelDir := filepath.Join(currentDir, "xlsx")
	sqlDir := filepath.Join(currentDir, "SQLTable")
	sqlDataDir := filepath.Join(currentDir, "SQLData")

	if _, err := os.Stat(sqlDir); os.IsNotExist(err) {
		os.Mkdir(sqlDir, 0755)
	}

	if _, err := os.Stat(sqlDataDir); os.IsNotExist(err) {
		os.Mkdir(sqlDataDir, 0755)
	}

	files, err := os.ReadDir(excelDir)
	if err != nil {
		logError(err, "Error membaca direktori xlsx")
		return
	}

	totalFiles = len(files)
	sem := make(chan struct{}, runtime.NumCPU())

	logRun("Mulai memproses file-file Excel.")
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".xlsx" {
			wg.Add(1)
			sem <- struct{}{}
			go processFile(filepath.Join(excelDir, file.Name()), sem, sqlDir, sqlDataDir)
		}
	}

	wg.Wait()
	logRun("Selesai memproses file-file Excel.")
	fmt.Println("Proses selesai.")

	/* proses pembuatan tabel database */
	fmt.Print("Apakah akan melanjutkan membuat tabel atau tabel-tabel di database? (Ya/Tidak, default Ya): ")

	var oCreateDB string
	fmt.Scanln(&oCreateDB)

	if strings.TrimSpace(strings.ToLower(oCreateDB)) == "tidak" {
		fmt.Println("Program dihentikan.")
		return
	}

	if _, err := os.Stat(dbConfigPath); os.IsNotExist(err) {
		fmt.Println("File konfigurasi database tidak ditemukan. Membuat file db.cfg...")
		err := createDefaultDBConfig()
		if err != nil {
			logError(err, "Gagal membuat file template konfigurasi database.")
		}
		fmt.Println("File db.cfg telah dibuat. Silakan ubah file db.cfg yang telah dibuat.")
		logError(err, "File konfigurasi database tidak ditemukan dan telah dibuat.")
		return
	}

	dbConfig, err := readDBConfig(dbConfigPath)
	if err != nil {
		logError(err, "Gagal membaca file konfigurasi database.")
		return
	}

	logRun("Mulai membuat koneksi ke database")
	// Create connection pool ...
	db, err := createDBConnection(dbConfig)
	if err != nil {
		logError(err, "Gagal membuat koneksi ke database.")
		return
	} else {
		logRun("Sukses membuat koneksi ke database.")
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			logError(err, "Gagal menutup koneksi ke database.")
		}
	}(db)
	logRun("Selesai membuat koneksi ke database")

	// Process SQL Table files ...
	processSQLTableFiles(db, sqlDir)
	fmt.Println("Proses pembuatan tabel database telah selesai.")

	/* proses pengisian data dari file-file Excel ke database */
	fmt.Print("Apakah akan melanjutkan pengisian data dari file-file Excel ke database? (Ya/Tidak, default Ya): ")

	var oFillDB string
	fmt.Scanln(&oFillDB)

	if strings.TrimSpace(strings.ToLower(oFillDB)) == "tidak" {
		fmt.Println("Program dihentikan.")
		return
	}
	// Process SQL Data files ...
	//processSQLDataFiles(db, sqlDataDir)
	processSQLDataFiles(db)
	fmt.Println("Proses pengisian data dari file-file Excel ke database telah selesai.")
	logRun("Program selesai bekerja.")
}
