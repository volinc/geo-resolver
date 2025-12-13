FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project file and restore dependencies
COPY ["GeoResolver.csproj", "./"]
RUN dotnet restore "GeoResolver.csproj"

# Copy everything else and build
COPY . .

# Publish for linux-x64 (standard platform for containers)
# For Native AOT, we use linux-x64 as it's the most widely supported
RUN dotnet publish "GeoResolver.csproj" -c Release -o /app/publish \
        -r linux-x64 --self-contained false \
        /p:UseAppHost=false /p:PublishTrimmed=true /p:TrimMode=full

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

# Set timezone data for Linux
ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV ASPNETCORE_URLS=http://+:8080

EXPOSE 8080

ENTRYPOINT ["dotnet", "GeoResolver.dll"]

