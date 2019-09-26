# bounce the docker volume vlingo-symbio-jdbc-postgres and vlingo-symbio-jdbc-mysql in the docker-compose.yml
docker-compose -p "dev" down
docker volume rm vlingo-symbio-jdbc-postgres
docker volume rm vlingo-symbio-jdbc-mysql
docker-compose -p "dev" up -d
