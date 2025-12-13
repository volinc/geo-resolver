FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy project file and restore dependencies
COPY ["GeoResolver.csproj", "./"]
RUN dotnet restore "GeoResolver.csproj"

# Copy everything else and build
COPY . .
RUN dotnet publish "GeoResolver.csproj" -c Release -o /app/publish

# Runtime stage - using chiseled Ubuntu image (ultra-lightweight, non-root by default)
FROM mcr.microsoft.com/dotnet/aspnet:10.0-noble-chiseled
WORKDIR /app

# Note: libgssapi_krb5 warning is non-critical - it's only needed for Kerberos auth
# For password authentication (which we use), these libraries are optional
# If needed, libraries can be copied from runtime-deps stage, but chiseled images
# have limited filesystem structure, so we skip this for simplicity

# Chiseled images already include non-root user 'app' with UID 1654
# Copy published application
COPY --from=build /app/publish .

# Set timezone and environment variables
ENV TZ=UTC
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true
ENV ASPNETCORE_URLS=http://+:8080

# Chiseled images run as non-root user by default, but we explicitly set it
USER app

EXPOSE 8080

ENTRYPOINT ["dotnet", "GeoResolver.dll"]

