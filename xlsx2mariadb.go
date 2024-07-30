package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/xuri/excelize/v2"
)

// Config holds the configuration read from db.cfg
type Config struct {
	Username string
	Password string
	Database string
	Host     string
	Port     string
	ExcelDir string
}

// readConfig reads the configuration from db.cfg
func readConfig() (Config, error) {
	file, err := os.Open("db.cfg")
	if err != nil {
		return Config{}, fmt.Errorf("error opening config file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if len(lines) < 6 {
		return Config{}, fmt.Errorf("config file format invalid")
	}

	config := Config{
		Username: lines[0],
		Password: lines[1],
		Database: lines[2],
		Host:     lines[3],
		Port:     lines[4],
		ExcelDir: lines[5],
	}

	return config, nil
}

// connectDB creates a connection pool to the MariaDB database
func connectDB(cfg Config) (*sql.DB, error) {
	var dsn string
	if runtime.GOOS == "linux" && cfg.Host == "localhost" {
		dsn = fmt.Sprintf("%s:%s@unix(/var/run/mysqld/mysqld.sock)/%s",
			cfg.Username, cfg.Password, cfg.Database)
	} else {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}
	return db, nil
}

// sanitizeString removes non-alphanumeric characters from a string and converts to lowercase
func sanitizeString(input string) string {
	reg, _ := regexp.Compile("[^a-zA-Z0-9]+")
	return strings.ToLower(reg.ReplaceAllString(input, ""))
}

// sanitizeSQLValue ensures SQL values are properly escaped and safe
func sanitizeSQLValue(value string) string {
	value = strings.ReplaceAll(value, "'", "''")
	return value
}

// readExcelFiles reads all Excel files from the specified directory
func readExcelFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.xlsx"))
	if err != nil {
		return nil, fmt.Errorf("error reading excel files: %v", err)
	}
	return files, nil
}

// isUniqueColumn checks if a column contains unique values
func isUniqueColumn(columnData []string) bool {
	uniqueValues := make(map[string]bool)
	for _, value := range columnData {
		if _, exists := uniqueValues[value]; exists {
			return false
		}
		uniqueValues[value] = true
	}
	return true
}

// processExcelFile processes an individual Excel file and generates SQL scripts
func processExcelFile(db *sql.DB, wg *sync.WaitGroup, filePath string, results chan<- string, errorLog *os.File) {
	defer wg.Done()

	start := time.Now()
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		results <- fmt.Sprintf("error opening excel file %s: %v", filePath, err)
		return
	}

	sheetName := f.GetSheetName(0)
	rows, err := f.GetRows(sheetName)
	if err != nil {
		results <- fmt.Sprintf("error reading rows from sheet %s in file %s: %v", sheetName, filePath, err)
		return
	}

	if len(rows) < 2 {
		results <- fmt.Sprintf("not enough data in file %s", filePath)
		return
	}

	tableName := sanitizeString(strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)))
	createSQLFileName := filepath.Join("hasil", fmt.Sprintf("%sTbl.sql", tableName))
	dataSQLFileName := filepath.Join("hasil", fmt.Sprintf("%sData.sql", tableName))
	errorSQLFileName := filepath.Join("hasil", fmt.Sprintf("%sData_cek.sql", tableName))

	// Create "hasil" directory if it doesn't exist
	if _, err := os.Stat("hasil"); os.IsNotExist(err) {
		err = os.Mkdir("hasil", 0755)
		if err != nil {
			results <- fmt.Sprintf("error creating directory 'hasil': %v", err)
			return
		}
	}

	// Remove existing files if they exist
	if _, err := os.Stat(createSQLFileName); err == nil {
		err = os.Remove(createSQLFileName)
		if err != nil {
			results <- fmt.Sprintf("error removing existing SQL file %s: %v", createSQLFileName, err)
			return
		}
	}
	if _, err := os.Stat(dataSQLFileName); err == nil {
		err = os.Remove(dataSQLFileName)
		if err != nil {
			results <- fmt.Sprintf("error removing existing SQL file %s: %v", dataSQLFileName, err)
			return
		}
	}
	if _, err := os.Stat(errorSQLFileName); err == nil {
		err = os.Remove(errorSQLFileName)
		if err != nil {
			results <- fmt.Sprintf("error removing existing SQL file %s: %v", errorSQLFileName, err)
			return
		}
	}

	createSQLFile, err := os.Create(createSQLFileName)
	if err != nil {
		results <- fmt.Sprintf("error creating SQL file %s: %v", createSQLFileName, err)
		return
	}
	defer createSQLFile.Close()

	dataSQLFile, err := os.Create(dataSQLFileName)
	if err != nil {
		results <- fmt.Sprintf("error creating SQL file %s: %v", dataSQLFileName, err)
		return
	}
	defer dataSQLFile.Close()

	errorSQLFile, err := os.Create(errorSQLFileName)
	if err != nil {
		results <- fmt.Sprintf("error creating SQL file %s: %v", errorSQLFileName, err)
		return
	}
	defer errorSQLFile.Close()

	headers := rows[0]
	columnNames := []string{}
	columnLengths := make([]int, len(headers))
	columnData := make([][]string, len(headers))
	for _, header := range headers {
		columnName := sanitizeString(header)
		columnNames = append(columnNames, columnName)
	}

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName)
	columns := []string{}
	for i, columnName := range columnNames {
		columnType := "VARCHAR(255)" // Default type
		for _, row := range rows[1:] {
			if len(row) > i {
				value := row[i]
				length := len(value)
				if length > columnLengths[i] {
					columnLengths[i] = length
				}
				columnData[i] = append(columnData[i], value)
				if _, err := strconv.Atoi(value); err == nil {
					if length <= 10 {
						columnType = "INT"
					} else {
						columnType = fmt.Sprintf("VARCHAR(%d)", columnLengths[i])
					}
				} else {
					columnType = fmt.Sprintf("VARCHAR(%d)", columnLengths[i])
				}
			}
		}
		columnDefinition := fmt.Sprintf("%s %s COMMENT '%s'", columnName, columnType, headers[i])
		if isUniqueColumn(columnData[i]) {
			columnDefinition += " UNIQUE"
		}
		columns = append(columns, columnDefinition)
	}
	createSQL += strings.Join(columns, ", ") + fmt.Sprintf(") COMMENT = '%s';", strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)))
	_, err = createSQLFile.WriteString(createSQL)
	if err != nil {
		results <- fmt.Sprintf("error writing to SQL file %s: %v", createSQLFileName, err)
		return
	}

	// Prepare insert statements
	dataSQLFile.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName) + strings.Join(columnNames, ", ") + ") VALUES\n")

	var errorRows []string
	batchSize := 10
	for i, row := range rows[1:] {
		if len(row) != len(columnNames) {
			for len(row) < len(columnNames) {
				row = append(row, "NA")
			}
		}
		values := []string{}
		for _, value := range row {
			values = append(values, fmt.Sprintf("'%s'", sanitizeSQLValue(value)))
		}
		if len(values) != len(columnNames) {
			errorRows = append(errorRows, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, strings.Join(columnNames, ", "), strings.Join(values, ", ")))
			continue
		}
		if i > 0 && i%batchSize == 0 {
			dataSQLFile.WriteString(";\n")
			dataSQLFile.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName) + strings.Join(columnNames, ", ") + ") VALUES\n")
		}
		dataSQLFile.WriteString("(" + strings.Join(values, ", ") + ")")
		if (i+1)%batchSize != 0 {
			dataSQLFile.WriteString(",\n")
		} else {
			dataSQLFile.WriteString("\n")
		}
	}
	dataSQLFile.WriteString(";\n")

	// Write error rows to separate file
	if len(errorRows) > 0 {
		for _, errorRow := range errorRows {
			_, err = errorSQLFile.WriteString(errorRow + "\n")
			if err != nil {
				results <- fmt.Sprintf("error writing to error SQL file %s: %v", errorSQLFileName, err)
				return
			}
		}
	}

	duration := time.Since(start)
	results <- fmt.Sprintf("Processed file %s in %v", filePath, duration)
}

// executeSQLFiles executes the SQL files in the specified order
func executeSQLFiles(db *sql.DB, sqlFiles []string, errorLog *os.File) error {
	for _, sqlFile := range sqlFiles {
		start := time.Now()

		file, err := os.Open(sqlFile)
		if err != nil {
			return fmt.Errorf("error opening SQL file %s: %v", sqlFile, err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var query string
		for scanner.Scan() {
			line := scanner.Text()
			query += line
			if strings.HasSuffix(line, ";") {
				query = strings.TrimSpace(query)
				if query == "" {
					continue
				}

				_, err := db.Exec(query)
				if err != nil {
					errorMessage := fmt.Sprintf("[%v] error executing query in file %s: %v\n", time.Now(), sqlFile, err)
					log.Printf(errorMessage)
					if _, err := errorLog.WriteString(errorMessage); err != nil {
						return fmt.Errorf("error writing to error log: %v", err)
					}
					continue
				}
				query = ""
			}
		}

		duration := time.Since(start)
		fmt.Printf("Executed file %s in %v\n", sqlFile, duration)
	}
	return nil
}

func main() {
	start := time.Now()

	config, err := readConfig()
	if err != nil {
		log.Fatalf("Error reading config: %v", err)
	}

	db, err := connectDB(config)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()

	errorLogFile, err := os.OpenFile("error.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error opening error log file: %v", err)
	}
	defer errorLogFile.Close()

	files, err := readExcelFiles(config.ExcelDir)
	if err != nil {
		log.Fatalf("Error reading Excel files: %v", err)
	}

	var wg sync.WaitGroup
	results := make(chan string, len(files))

	for _, file := range files {
		wg.Add(1)
		go processExcelFile(db, &wg, file, results, errorLogFile)
	}

	wg.Wait()
	close(results)

	sqlFiles := []string{}
	for result := range results {
		fmt.Println(result)
		if strings.Contains(result, "Processed file") {
			fileName := strings.Split(result, " ")[2]
			baseName := sanitizeString(strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName)))
			sqlFiles = append(sqlFiles, filepath.Join("hasil", fmt.Sprintf("%sTbl.sql", baseName)))
			sqlFiles = append(sqlFiles, filepath.Join("hasil", fmt.Sprintf("%sData.sql", baseName)))
		}
	}

	err = executeSQLFiles(db, sqlFiles, errorLogFile)
	if err != nil {
		log.Fatalf("Error executing SQL files: %v", err)
	}

	totalDuration := time.Since(start)
	fmt.Printf("Total time taken: %v\n", totalDuration)
}
