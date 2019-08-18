# bounce the docker volume vlingo-symbio-jdbc-postgres in the docker-compose.yml
docker-compose -p "dev" down
docker volume rm vlingo-symbio-jdbc-postgres
docker-compose -p "dev" up -d
