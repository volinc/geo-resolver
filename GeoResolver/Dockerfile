#FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled AS base
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER app
WORKDIR /app
EXPOSE 8080

ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV ASPNETCORE_URLS=http://+:8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["GeoResolver/GeoResolver.csproj", "GeoResolver/"]
RUN dotnet restore "GeoResolver/GeoResolver.csproj"
COPY . .
WORKDIR "/src/GeoResolver"
RUN dotnet build "./GeoResolver.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./GeoResolver.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "GeoResolver.dll"]
