# Forebag.Kafka

Клиент Kafka для .NET

## Forebag.Kafka.Demo

Проекты Forebag.Kafka.Demo.Producer и Forebag.Kafka.Demo.Consumer демонстрируют работу клиента Kafka.

### Запуск Demo

1. Запустить docker с kafka `docker-compose up kafka`.
2. Запустить проект с consumer `dotnet run --project src/Forebag.Kafka.Demo.Consumer`.
3. Запустить проект с producer `dotnet run --project src/Forebag.Kafka.Demo.Producer`.

Для отправки сообщений, в запущенном producer ввести key/value в два слова через пробел, например `key1 UserName` (enter).

### Запуск тестов

#### Запуск локально

1. Запустить docker с kafka `docker-compose up kafka`.
2. Запустить тесты `dotnet test --logger:trx`.

#### Запуск в docker

Запустить docker с тестами `docker-compose up --build`.
