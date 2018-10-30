# start postgres in docker with a clean data for test
docker-compose down
docker volume rm $(docker volume ls -q)
docker-compose up
