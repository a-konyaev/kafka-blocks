@echo off

echo creating (if needed) and starting containers...
docker-compose up -d

echo running containers:
docker ps -a
