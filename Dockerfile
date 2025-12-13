FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG TARGETARCH
WORKDIR /src

# Copy project file and restore dependencies
COPY ["GeoResolver.csproj", "./"]
RUN dotnet restore "GeoResolver.csproj"

# Copy everything else and build
COPY . .

# Determine target RID based on architecture for multi-platform support
RUN case ${TARGETARCH} in \
    amd64) TARGETRID=linux-x64 ;; \
    arm64) TARGETRID=linux-arm64 ;; \
    arm) TARGETRID=linux-arm ;; \
    *) TARGETRID=linux-x64 ;; \
    esac && \
    dotnet publish "GeoResolver.csproj" -c Release -o /app/publish \
        -r ${TARGETRID} --self-contained false \
        /p:UseAppHost=false /p:PublishTrimmed=true /p:TrimMode=full

# Runtime stage - use TARGETARCH for multi-platform
FROM --platform=$TARGETPLATFORM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /app/publish .

# Set timezone data for Linux
ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false
ENV ASPNETCORE_URLS=http://+:8080

EXPOSE 8080

ENTRYPOINT ["dotnet", "GeoResolver.dll"]

