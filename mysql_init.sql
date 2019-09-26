CREATE DATABASE vlingo_test;
CREATE USER 'vlingo_test'@'localhost' IDENTIFIED BY 'vlingo123';
GRANT ALL ON *.* TO 'vlingo_test'@'localhost';
FLUSH PRIVILEGES;
