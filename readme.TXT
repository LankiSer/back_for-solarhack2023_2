docker build -t minio-server .
docker run -p 9000:9000 -p 9001:9001 minio-server server /data --console-address ":9001"

использовать следующие URL-адреса для доступа к MinIO:

S3-API: http://localhost:9000
Консоль MinIO: http://localhost:9001
