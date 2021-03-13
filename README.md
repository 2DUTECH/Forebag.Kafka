# Forebag.Kafka

Клиент Kafka для .NET

### Запуск тестов

#### Запуск локально

1. Запустить docker с kafka `docker-compose up kafka`.
2. Запустить тесты `dotnet test --logger:trx`.

#### Запуск в docker

Запустить docker с тестами `docker-compose up --build .`
