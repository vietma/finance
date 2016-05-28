CREATE DATABASE finance;

CREATE USER 'financeAdmin'@'localhost' IDENTIFIED BY 'passW0rd1';

GRANT ALL PRIVILEGES ON finance.* TO 'financeAdmin'@'localhost';

FLUSH PRIVILEGES;
