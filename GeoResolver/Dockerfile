# Use distroless image for Native AOT (no .NET runtime needed)
FROM gcr.io/distroless/cc-debian12:nonroot AS base
WORKDIR /app
EXPOSE 8080

ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV ASPNETCORE_URLS=http://+:8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
# Install Native AOT prerequisites (clang and required libraries)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    clang \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY ["GeoResolver/GeoResolver.csproj", "GeoResolver/"]
RUN dotnet restore "GeoResolver/GeoResolver.csproj"
COPY . .
WORKDIR "/src/GeoResolver"
RUN dotnet build "./GeoResolver.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
# Publish as Native AOT with self-contained and full trimming
RUN dotnet publish "./GeoResolver.csproj" \
    -c $BUILD_CONFIGURATION \
    -o /app/publish \
    -r linux-x64 \
    --self-contained true \
    /p:PublishAot=true \
    /p:PublishTrimmed=true \
    /p:TrimMode=full

FROM base AS final
WORKDIR /app
# Copy the native executable (not a DLL)
COPY --from=publish /app/publish/GeoResolver .
# Copy configuration files
COPY --from=publish /app/publish/appsettings*.json ./
# Native AOT produces a single executable, no need for dotnet command
ENTRYPOINT ["./GeoResolver"]
