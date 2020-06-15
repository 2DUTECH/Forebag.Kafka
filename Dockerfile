FROM mcr.microsoft.com/dotnet/core/sdk:3.1 as base

WORKDIR /kafka
COPY *.sln ./
COPY /src ./src
COPY /tests ./tests

RUN dotnet build

# there is should be publish step (we should to implement this after setup CD on environment)
