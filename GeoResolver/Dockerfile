## Stage 1: build and publish Native AOT binary (using .NET SDK AOT image)
FROM mcr.microsoft.com/dotnet/sdk:10.0-noble-aot AS build

# TARGETARCH is provided automatically by Docker BuildKit / buildx
# (e.g. amd64, arm64). We use it to choose the correct .NET RID.
ARG TARGETARCH
ARG BUILD_CONFIGURATION=Release

WORKDIR /src

# Copy project file and restore dependencies
COPY ["GeoResolver/GeoResolver.csproj", "GeoResolver/"]
RUN dotnet restore "GeoResolver/GeoResolver.csproj"

# Copy the remaining source and publish as Native AOT
COPY . .
WORKDIR "/src/GeoResolver"
RUN case "$TARGETARCH" in \
        arm64) RID=linux-arm64 ;; \
        *)     RID=linux-x64 ;; \
    esac; \
    dotnet publish "./GeoResolver.csproj" \
        -c $BUILD_CONFIGURATION \
        -o /app/publish \
        -r "$RID" \
        --self-contained true \
        /p:PublishAot=true \
        /p:PublishTrimmed=true \
        /p:TrimMode=full


## Stage 2: minimal runtime image (self-contained Native AOT executable)
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS final

WORKDIR /app
EXPOSE 8080

ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV ASPNETCORE_URLS=http://+:8080

# Copy the native executable and configuration
COPY --from=build --chown=app:app /app/publish/GeoResolver .
COPY --from=build --chown=app:app /app/publish/appsettings*.json ./

# Run as non-root app user provided by .NET container images
USER app

# Native AOT produces a single executable, no need for dotnet command
ENTRYPOINT ["./GeoResolver"]
