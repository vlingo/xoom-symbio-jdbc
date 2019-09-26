# bounce the docker volume vlingo-symbio-jdbc-postgres and vlingo-symbio-jdbc-mysql in the docker-compose.yml
docker-compose -p "dev" down
docker volume rm vlingo-symbio-jdbc-postgres
docker volume rm vlingo-symbio-jdbc-mysql
docker-compose -p "dev" up -d

cwd=$(pwd)
echo ${cwd}
mysqlcmd=("'mysql -u root -p vlingo123 -e "${cwd}"/mysql_init.sql'")
echo "${mysqlcmd[@]}"
docker exec -it vlingo_mysql "${mysqlcmd[@]}"
