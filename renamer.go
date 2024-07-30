package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: namaprogram <directory> <oldString> <newString>")
		return
	}

	directory := os.Args[1]
	oldString := os.Args[2]
	newString := os.Args[3]

	err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Only process files
		if !info.IsDir() {
			oldName := filepath.Base(path)
			if strings.Contains(oldName, oldString) {
				newName := strings.Replace(oldName, oldString, newString, -1)
				newPath := filepath.Join(filepath.Dir(path), newName)
				err := os.Rename(path, newPath)
				if err != nil {
					return err
				}
				fmt.Printf("Renamed: %s -> %s\n", oldName, newName)
			}
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
