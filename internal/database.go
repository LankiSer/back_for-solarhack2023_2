// internal/database.go
package internal

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// Stats представляет структуру данных для отображения таблицы в базе данных
type Stats struct {
	OrgName     string    `db:"org_name"`
	SrcIP       string    `db:"src_ip"`
	SrcPort     int       `db:"src_port"`
	DstIP       string    `db:"dst_ip"`
	DstPort     int       `db:"dst_port"`
	PacketCount int       `db:"packets_count"`
	ByteCount   int       `db:"bytes_count"`
	Timestamp   time.Time `db:"timestamp_column"`
}

// GetStats выполняет SQL-запрос и возвращает результаты
func GetStats(db *sql.DB, orgName string, startTime, endTime time.Time) ([]Stats, error) {
	const query = `
		SELECT org_name, src_ip, src_port, dst_ip, dst_port, packets_count, bytes_count, timestamp_column
		FROM logs
		WHERE org_name = $1 AND timestamp_column >= $2 AND timestamp_column <= $3
	`

	rows, err := db.Query(query, orgName, startTime, endTime)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var resultStats []Stats
	for rows.Next() {
		var stat Stats
		err := rows.Scan(&stat.OrgName, &stat.SrcIP, &stat.SrcPort, &stat.DstIP, &stat.DstPort, &stat.PacketCount, &stat.ByteCount, &stat.Timestamp)
		if err != nil {
			return nil, err
		}
		resultStats = append(resultStats, stat)
	}

	return resultStats, nil
}

// OpenDB открывает соединение с базой данных
func OpenDB() (*sql.DB, error) {
	connectionString := "host=127.0.0.1 user=root password=root dbname=test sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return db, nil
}
