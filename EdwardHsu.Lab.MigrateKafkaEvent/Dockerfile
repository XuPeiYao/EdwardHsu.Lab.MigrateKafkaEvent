#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:3.1 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:3.1 AS build
WORKDIR /src
COPY ["EdwardHsu.Lab.MigrateKafkaEvent/EdwardHsu.Lab.MigrateKafkaEvent.csproj", "EdwardHsu.Lab.MigrateKafkaEvent/"]
RUN dotnet restore "EdwardHsu.Lab.MigrateKafkaEvent/EdwardHsu.Lab.MigrateKafkaEvent.csproj"
COPY . .
WORKDIR "/src/EdwardHsu.Lab.MigrateKafkaEvent"
RUN dotnet build "EdwardHsu.Lab.MigrateKafkaEvent.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "EdwardHsu.Lab.MigrateKafkaEvent.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "EdwardHsu.Lab.MigrateKafkaEvent.dll"]