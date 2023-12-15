// internal/reporting.go

package internal

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/minio/minio-go/v7"
	"golang.org/x/net/context"
	"log"
	"os"
	"strings"
)

// FilterColumnsFromCSV копирует указанные столбцы из одного CSV-файла в другой
func FilterColumnsFromCSV(inputPath, outputPath string, columnsToKeep []string) error {
	// Считывание данных из исходного CSV-файла
	data, err := ReadCSV(inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input CSV file: %v", err)
	}

	// Открытие файла для записи фильтрованного отчета
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output CSV file: %v", err)
	}
	defer outputFile.Close()

	// Инициализация записи в CSV-файл
	outputWriter := csv.NewWriter(outputFile)
	defer outputWriter.Flush()

	// Формирование заголовка фильтрованного отчета
	filteredHeader := make([]string, 0, len(columnsToKeep))
	for _, col := range columnsToKeep {
		filteredHeader = append(filteredHeader, col)
	}

	// Запись заголовка в фильтрованный отчет
	if err := outputWriter.Write(filteredHeader); err != nil {
		return fmt.Errorf("failed to write filtered header: %v", err)
	}

	// Отображение индексов колонок в массиве данных
	colIndexes := make(map[string]int)
	for i, col := range data[0] {
		colIndexes[col] = i
	}

	// Формирование новых записей только с указанными столбцами
	for _, row := range data[1:] {
		newRow := make([]string, 0, len(columnsToKeep))
		for _, col := range columnsToKeep {
			if index, ok := colIndexes[col]; ok {
				newRow = append(newRow, row[index])
			}
		}

		// Запись строки в фильтрованный отчет
		if err := outputWriter.Write(newRow); err != nil {
			return fmt.Errorf("failed to write filtered row: %v", err)
		}
	}

	log.Printf("Filtered report saved to %s\n", outputPath)
	return nil
}

// ReadCSV считывает данные из CSV-файла
func ReadCSV(filePath string) ([][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}

// SaveToCSV сохраняет данные в CSV-файле
func SaveToCSV(data [][]string, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range data {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	log.Printf("Data saved to %s\n", filePath)
	return nil
}

// FilterColumns оставляет только указанные столбцы в данных
func FilterColumns(data [][]string, columnsToKeep []string) [][]string {
	if len(data) == 0 {
		return nil
	}

	header := data[0]
	keepIndices := make([]int, 0, len(columnsToKeep))

	// Находим индексы столбцов, которые нужно оставить
	for _, col := range columnsToKeep {
		for i, headerCol := range header {
			if headerCol == col {
				keepIndices = append(keepIndices, i)
			}
		}
	}

	// Создаем новый массив данных с выбранными столбцами
	filteredData := make([][]string, len(data))
	for i, row := range data {
		filteredRow := make([]string, 0, len(keepIndices))
		for _, index := range keepIndices {
			if index < len(row) {
				filteredRow = append(filteredRow, row[index])
			}
		}
		filteredData[i] = filteredRow
	}

	return filteredData
}

// FilterColumnsFromMinIO копирует указанные столбцы из одного CSV-файла в MinIO в другой
func FilterColumnsFromMinIO(minioClient *minio.Client, bucketName, inputPath, outputFileID string, columnsToKeep []string) error {
	// Считывание данных из CSV-файла в MinIO
	data, err := ReadCSVFromMinIO(minioClient, bucketName, inputPath)
	if err != nil {
		return fmt.Errorf("failed to read input CSV file from MinIO: %v", err)
	}

	// Формирование имени файла filtered_output.csv с уникальным идентификатором
	outputFileName := fmt.Sprintf("filtered_output_ID%s.csv", outputFileID)
	outputFilePath := fmt.Sprintf("filtered/%s", outputFileName)

	// Инициализация буфера для записи в MinIO
	var outputBuffer bytes.Buffer

	// Формирование новых записей только с указанными столбцами
	outputData := [][]string{columnsToKeep} // заголовок
	for _, row := range data[1:] {
		newRow := make([]string, 0, len(columnsToKeep))
		for _, col := range columnsToKeep {
			col = strings.TrimSpace(col)
			if index := indexOf(data[0], col); index != -1 {
				newRow = append(newRow, row[index])
			}
		}
		outputData = append(outputData, newRow)
	}

	// Запись данных в буфер
	writer := csv.NewWriter(&outputBuffer)
	if err := writer.WriteAll(outputData); err != nil {
		return fmt.Errorf("failed to write data to buffer: %v", err)
	}

	// Загрузка файла filtered_output.csv в MinIO с использованием уникального идентификатора в имени объекта
	_, err = minioClient.PutObject(context.Background(), bucketName, outputFilePath, &outputBuffer, int64(outputBuffer.Len()), minio.PutObjectOptions{
		ContentType: "application/csv",
	})
	if err != nil {
		return fmt.Errorf("failed to upload filtered report to MinIO: %v", err)
	}

	// Удаление файла из корневой папки компьютера
	err = os.Remove(outputFileName)
	if err != nil {
		log.Printf("Failed to remove file from the local filesystem: %v\n", err)
	}

	log.Printf("Filtered report saved to %s\n", outputFilePath)
	return nil
}

// ReadCSVFromMinIO считывает данные из CSV-файла в MinIO
func ReadCSVFromMinIO(minioClient *minio.Client, bucketName, filePath string) ([][]string, error) {
	reader, err := minioClient.GetObject(context.Background(), bucketName, filePath, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object from MinIO: %v", err)
	}
	defer reader.Close()

	csvReader := csv.NewReader(reader)
	data, err := csvReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV from MinIO: %v", err)
	}

	return data, nil
}

// indexOf возвращает индекс элемента в строковом слайсе, -1 если элемент не найден
func indexOf(slice []string, target string) int {
	for i, s := range slice {
		if s == target {
			return i
		}
	}
	return -1
}
