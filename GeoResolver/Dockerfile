# Use Microsoft .NET chiseled runtime-deps image for Native AOT (no .NET runtime needed)
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS base
WORKDIR /app
EXPOSE 8080

ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV ASPNETCORE_URLS=http://+:8080

FROM mcr.microsoft.com/dotnet/sdk:10.0-noble AS build
ARG BUILD_CONFIGURATION=Release
# Install Native AOT prerequisites for cross-compilation (ARM to x86_64)
# Install clang, lld linker, and x86_64 cross-compilation tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    clang \
    lld \
    zlib1g-dev \
    binutils-x86-64-linux-gnu \
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
# Use lld linker for cross-platform compatibility (ARM to x86_64)
# Set environment variables to use lld linker
ENV CC=clang
ENV CXX=clang++
ENV LDFLAGS="-fuse-ld=lld"
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
COPY --from=publish --chown=app:app /app/publish/GeoResolver .
# Copy configuration files
COPY --from=publish --chown=app:app /app/publish/appsettings*.json ./
# Run as non-root app user provided by .NET container images
USER app
# Native AOT produces a single executable, no need for dotnet command
ENTRYPOINT ["./GeoResolver"]
