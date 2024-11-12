cd docker
docker-compose up -d

psql -h localhost -U postgres -d movie_analytics -f ../sql/create_tables.sql

python main.py

KT log 
# Xem logs của tất cả services
docker-compose logs

# Xem logs của service cụ thể
docker-compose logs [service-name]

MinIO
    Username: minio
    Password: minio123

Database Access
PostgreSQL connection info:

    Host: localhost
    Port: 5432
    Database: movie_analytics
    Username: postgres
    Password: postgres

