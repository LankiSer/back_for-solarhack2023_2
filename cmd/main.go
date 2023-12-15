// main.go

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"awesomeProject/internal"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	minioEndpoint  = "localhost:9000"
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioBucket    = "bucket"
)

const reportInterval = 1 * 1 * time.Second // неделя

var filteredFiles []string

// Define a new struct to represent the JSON payload
type RequestData struct {
	OrgName   string `json:"orgName"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	Shablon   string `json:"shablon"`
}

func main() {
	// Открытие соединения с базой данных
	db, err := internal.OpenDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Запуск HTTP-сервера для обработки запросов от фронтэнда
	http.HandleFunc("/processData",
		// In your handler function, decode the JSON using the new struct
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				var requestData RequestData

				// Decode the JSON payload
				if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
					http.Error(w, fmt.Sprintf("Failed to decode JSON: %v", err), http.StatusBadRequest)
					return
				}

				// Используем requestData вместо r.FormValue
				orgName := requestData.OrgName
				startTimeString := requestData.StartTime
				endTimeString := requestData.EndTime
				shablon := requestData.Shablon

				// Преобразование временных строк в объекты time.Time
				startTime, err := time.Parse(time.RFC3339, startTimeString)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to parse startTime: %v", err), http.StatusBadRequest)
					return
				}

				endTime, err := time.Parse(time.RFC3339, endTimeString)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to parse endTime: %v", err), http.StatusBadRequest)
					return
				}

				// Вызов функции из пакета internal
				resultStats, err := internal.GetStats(db, orgName, startTime, endTime)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to get stats: %v", err), http.StatusInternalServerError)
					return
				}

				// Создание уникальных идентификаторов для файлов
				outputFileID := generateUniqueID()
				filteredOutputFileID := generateUniqueID()

				// Формирование имени файла output.csv с уникальным идентификатором
				outputFileName := fmt.Sprintf("output_ID%s.csv", outputFileID)
				outputFilePath := fmt.Sprintf("output/%s", outputFileName)

				// Создание CSV-файла
				file, err := os.Create(outputFileName)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to create output file: %v", err), http.StatusInternalServerError)
					return
				}
				defer file.Close()

				// Создание записи в CSV-файле
				writer := csv.NewWriter(file)
				defer writer.Flush()

				// Запись заголовка
				header := []string{"org_name", "SrcIP", "SrcPort", "DstIP", "DstPort", "PacketCount", "ByteCount", "Timestamp"}
				if err := writer.Write(header); err != nil {
					http.Error(w, fmt.Sprintf("Failed to write header: %v", err), http.StatusInternalServerError)
					return
				}

				// Запись данных
				for _, stat := range resultStats {
					record := []string{
						fmt.Sprintf(stat.OrgName),
						stat.SrcIP, fmt.Sprintf("%d", stat.SrcPort),
						stat.DstIP, fmt.Sprintf("%d", stat.DstPort),
						fmt.Sprintf("%d", stat.PacketCount),
						fmt.Sprintf("%d", stat.ByteCount),
						formatTime(stat.Timestamp),
					}
					if err := writer.Write(record); err != nil {
						http.Error(w, fmt.Sprintf("Failed to write record: %v", err), http.StatusInternalServerError)
						return
					}
				}

				writer.Flush()

				// Инициализация клиента MinIO
				minioClient, err := minio.New(minioEndpoint, &minio.Options{
					Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
					Secure: false, // Используйте true для HTTPS
				})
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to initialize MinIO client: %v", err), http.StatusInternalServerError)
					return
				}

				// Загрузка файла output.csv в ведро MinIO с использованием уникального идентификатора в имени объекта
				_, err = minioClient.FPutObject(context.Background(), minioBucket, outputFilePath, outputFileName, minio.PutObjectOptions{})
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to upload file to MinIO: %v", err), http.StatusInternalServerError)
					return
				}

				// Функция для генерации и отправки отчета
				generateAndSendReport := func() {
					// Вызываем функцию из пакета internal для создания фильтрованного отчета
					shablonArray := strings.Split(shablon, ",")
					filteredOutputFilePath := fmt.Sprintf("filtered_output_ID%s.csv", filteredOutputFileID)
					err := internal.FilterColumnsFromMinIO(minioClient, minioBucket, outputFilePath, filteredOutputFileID, shablonArray)
					if err != nil {
						log.Println("Error generating filtered report:", err)
						http.Error(w, fmt.Sprintf("Error generating filtered report: %v", err), http.StatusInternalServerError)
						return
					} else {
						log.Printf("Filtered report generated successfully. Saved to %s\n", filteredOutputFilePath)
						// Добавляем путь к фильтрованному файлу в массив
						filteredFiles = append(filteredFiles, filteredOutputFilePath)
					}

					// После выполнения задачи - завершаем скрипт
					os.Remove(outputFilePath) // Удаление файла из корневой папки
				}

				// Используем time.AfterFunc для генерации событий с интервалом времени
				timer := time.AfterFunc(reportInterval, func() {
					// Вызываем функцию при срабатывании таймера
					generateAndSendReport()
				})

				// Ждем завершения программы
				select {
				case <-timer.C:
					// Таймер выполнил свою функцию, ничего не делаем
				}

				log.Println("Columns filtered and saved successfully.")
				// Завершение записи и сброс буфера перед закрытием файла
				writer.Flush()

				log.Println("Query executed successfully. Data written to", outputFileName)

				// Оставляем основную программу активной (например, слушаем HTTP-запросы)
				select {}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Processing started."))
			} else {
				http.Error(w, "Invalid request method. Only POST is allowed.", http.StatusMethodNotAllowed)
			}
		})

	// Новый обработчик для предоставления массива фильтрованных файлов
	http.HandleFunc("/getFilteredFiles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			// Отправляем массив фильтрованных файлов на фронтенд
			json.NewEncoder(w).Encode(filteredFiles)
		} else {
			http.Error(w, "Invalid request method. Only GET is allowed.", http.StatusMethodNotAllowed)
		}
	})

	// Запуск HTTP-сервера
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func formatTime(t time.Time) string {
	// Форматируем время в строку
	return t.Format("2006-01-02 15:04:05")
}

func generateUniqueID() string {
	// Генерация уникального идентификатора с использованием UUID
	id := uuid.New()
	return id.String()
}
