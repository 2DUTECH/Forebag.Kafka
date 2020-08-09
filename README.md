# Forebag.Kafka

## How to run tests

### Local

1. Run docker with kafka `docker-compose up kafka`.
2. Run tests `dotnet test -l "console;verbosity=detailed"`.

### Inside docker

Run docker with building of sln `docker-compose up --build`.
