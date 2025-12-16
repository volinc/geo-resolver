# Предложения по оптимизации и улучшению читаемости GeoResolver

## 1. Оптимизация GeoLocationService.cs

### Проблемы:
- Дублирование кода в методах `Find*ByPointAsync`
- Использование строковых SQL вместо параметризованных геометрических типов
- Отсутствие prepared statements для часто выполняемых запросов

### Решения:

#### 1.1. Использование параметризованных геометрических типов вместо строк

**Текущий код:**
```csharp
var point = $"POINT({longitude} {latitude})";
cmd.Parameters.AddWithValue("point", NpgsqlDbType.Text, point);
```

**Улучшенный код:**
```csharp
var point = new Point(longitude, latitude) { SRID = 4326 };
cmd.Parameters.AddWithValue("point", point);
```

Это:
- Безопаснее (нет SQL injection)
- Быстрее (PostgreSQL не парсит строку)
- Использует индексы эффективнее

#### 1.2. Выделение общего метода для пространственных запросов

**Предложение:**
```csharp
private async Task<T?> FindEntityByPointAsync<T>(
    string tableName,
    Func<NpgsqlDataReader, T> mapFunc,
    double latitude,
    double longitude,
    CancellationToken cancellationToken = default)
{
    await using var connection = _npgsqlDataSource.CreateConnection();
    await connection.OpenAsync(cancellationToken);

    var point = new Point(longitude, latitude) { SRID = 4326 };
    await using var cmd = new NpgsqlCommand($@"
        SELECT * FROM {tableName}
        WHERE ST_Contains(geometry, @point)
        LIMIT 1;", connection);

    cmd.Parameters.AddWithValue("point", point);
    
    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
    if (await reader.ReadAsync(cancellationToken))
    {
        return mapFunc(reader);
    }

    return default;
}
```

#### 1.3. Использование prepared statements

Для часто выполняемых запросов можно использовать prepared statements:
```csharp
// В конструкторе или при первом использовании
await cmd.PrepareAsync(cancellationToken);
```

## 2. Рефакторинг DatabaseWriterService.cs

### Проблемы:
- Файл слишком большой (~1600 строк)
- Много дублирования кода
- Сложно тестировать отдельные части

### Решения:

#### 2.1. Разделение на специализированные сервисы

**Предложение структуры:**
```
Services/
  DatabaseWriter/
    ICountryWriter.cs
    CountryWriter.cs
    IRegionWriter.cs
    RegionWriter.cs
    ICityWriter.cs
    CityWriter.cs
    BaseGeoWriter.cs (общая логика)
```

#### 2.2. Выделение констант

**Предложение:**
```csharp
public static class GeoJsonFieldNames
{
    public const string IsoA2 = "ISO_A2";
    public const string IsoA3 = "ISO_A3";
    public const string NameLocal = "name_local";
    public const string Name = "name";
    // ... и т.д.
}
```

#### 2.3. Использование стратегий для извлечения данных из GeoJSON

**Предложение:**
```csharp
public interface IGeoJsonPropertyExtractor
{
    string? ExtractIsoAlpha2(JsonElement properties);
    string? ExtractIsoAlpha3(JsonElement properties);
    string? ExtractName(JsonElement properties);
    // ...
}

public class NaturalEarthPropertyExtractor : IGeoJsonPropertyExtractor { }
public class OsmPropertyExtractor : IGeoJsonPropertyExtractor { }
```

## 3. Оптимизация производительности

### 3.1. Кэширование результатов запросов

Для часто запрашиваемых координат можно добавить кэш:
```csharp
private readonly IMemoryCache _cache;

public async Task<GeoLocationResponse> ResolveAsync(
    double latitude, 
    double longitude, 
    CancellationToken cancellationToken = default)
{
    var cacheKey = $"geo:{latitude:F6}:{longitude:F6}";
    if (_cache.TryGetValue(cacheKey, out GeoLocationResponse? cached))
    {
        return cached!;
    }

    var result = await ResolveInternalAsync(latitude, longitude, cancellationToken);
    _cache.Set(cacheKey, result, TimeSpan.FromMinutes(5));
    return result;
}
```

**Примечание:** Кэширование может быть нежелательно, если нужны актуальные данные. Используйте с осторожностью.

### 3.2. Batch-обработка для множественных запросов

Если API будет поддерживать batch-запросы:
```csharp
public async Task<IEnumerable<GeoLocationResponse>> ResolveBatchAsync(
    IEnumerable<(double Latitude, double Longitude)> coordinates,
    CancellationToken cancellationToken = default)
{
    // Использовать один запрос с IN или массивом точек
}
```

### 3.3. Использование материализованных представлений

Для часто запрашиваемых комбинаций можно создать материализованное представление:
```sql
CREATE MATERIALIZED VIEW geo_location_cache AS
SELECT 
    ST_GeomFromText('POINT(...)', 4326) as point,
    c.iso_alpha2_code,
    r.identifier as region_identifier,
    -- ...
FROM ...
```

## 4. Улучшение читаемости кода

### 4.1. Использование record types для промежуточных данных

**Вместо:**
```csharp
var citiesToUpdate = new List<(int Id, string Identifier, string Name)>();
```

**Использовать:**
```csharp
public record CityUpdateInfo(int Id, string Identifier, string Name);
var citiesToUpdate = new List<CityUpdateInfo>();
```

### 4.2. Выделение методов валидации

**Вместо:**
```csharp
if (string.IsNullOrWhiteSpace(isoAlpha2Code) || string.IsNullOrWhiteSpace(isoAlpha3Code))
{
    // ...
}
```

**Использовать:**
```csharp
private static bool IsValidIsoCodePair(string? alpha2, string? alpha3) =>
    !string.IsNullOrWhiteSpace(alpha2) && !string.IsNullOrWhiteSpace(alpha3);

if (!IsValidIsoCodePair(isoAlpha2Code, isoAlpha3Code))
{
    // ...
}
```

### 4.3. Использование Result types для обработки ошибок

**Предложение:**
```csharp
public record ImportResult(int Processed, int Skipped, IReadOnlyDictionary<string, int> SkipReasons);

public async Task<ImportResult> ImportCountriesFromGeoJsonAsync(...)
{
    // ...
    return new ImportResult(processed, skipped, skippedReasons);
}
```

### 4.4. Выделение SQL-запросов в отдельные классы

**Предложение:**
```csharp
public static class SqlQueries
{
    public const string FindCountryByPoint = @"
        SELECT id, iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry
        FROM countries
        WHERE ST_Contains(geometry, @point)
        LIMIT 1;";
    
    public const string InsertCountry = @"
        INSERT INTO countries (iso_alpha2_code, iso_alpha3_code, name_latin, wikidataid, geometry)
        VALUES (@isoAlpha2Code, @isoAlpha3Code, @name, @wikidataid, ST_GeomFromWKB(@geometry, 4326))
        ON CONFLICT (iso_alpha2_code) DO UPDATE
        SET iso_alpha3_code = EXCLUDED.iso_alpha3_code,
            name_latin = EXCLUDED.name_latin, 
            wikidataid = EXCLUDED.wikidataid,
            geometry = EXCLUDED.geometry;";
}
```

## 5. Улучшение обработки ошибок

### 5.1. Использование Result types

**Предложение:**
```csharp
public sealed class Result<T>
{
    public bool IsSuccess { get; }
    public T? Value { get; }
    public string? Error { get; }
    
    private Result(bool isSuccess, T? value, string? error)
    {
        IsSuccess = isSuccess;
        Value = value;
        Error = error;
    }
    
    public static Result<T> Success(T value) => new(true, value, null);
    public static Result<T> Failure(string error) => new(false, default, error);
}
```

### 5.2. Более информативные исключения

**Вместо:**
```csharp
throw new InvalidOperationException($"Country '{country.NameLatin}' must have both ISO codes");
```

**Использовать:**
```csharp
public class InvalidCountryDataException : InvalidOperationException
{
    public string CountryName { get; }
    public InvalidCountryDataException(string countryName, string reason) 
        : base($"Country '{countryName}' is invalid: {reason}")
    {
        CountryName = countryName;
    }
}
```

## 6. Тестируемость

### 6.1. Выделение зависимостей

**Текущий код:**
```csharp
private readonly NpgsqlDataSource _npgsqlDataSource;
```

**Улучшенный код:**
```csharp
public interface IDatabaseConnectionFactory
{
    Task<NpgsqlConnection> CreateConnectionAsync(CancellationToken cancellationToken);
}

// В тестах можно использовать моки
```

### 6.2. Использование IOptions для конфигурации

**Вместо:**
```csharp
const int logIntervalSeconds = 10;
const int logIntervalCount = 100;
```

**Использовать:**
```csharp
public class ImportOptions
{
    public int LogIntervalSeconds { get; set; } = 10;
    public int LogIntervalCount { get; set; } = 100;
}
```

## 7. Документация и комментарии

### 7.1. XML-документация для публичных методов

Все публичные методы должны иметь XML-документацию:
```csharp
/// <summary>
/// Resolves geo-location information for given coordinates.
/// </summary>
/// <param name="latitude">Latitude in decimal degrees (-90 to 90)</param>
/// <param name="longitude">Longitude in decimal degrees (-180 to 180)</param>
/// <param name="cancellationToken">Cancellation token</param>
/// <returns>Geo-location response with country, region, city, and timezone information</returns>
/// <exception cref="ArgumentOutOfRangeException">Thrown when coordinates are out of valid range</exception>
public async Task<GeoLocationResponse> ResolveAsync(...)
```

### 7.2. Выделение сложной логики в отдельные методы с понятными именами

**Вместо:**
```csharp
// 50 строк сложной логики извлечения данных из GeoJSON
```

**Использовать:**
```csharp
private Country ExtractCountryFromGeoJson(JsonElement feature)
{
    var properties = feature.GetProperty("properties");
    var isoCodes = ExtractIsoCodes(properties);
    var name = ExtractCountryName(properties);
    var wikidataId = ExtractWikidataId(properties);
    var geometry = ExtractGeometry(feature);
    
    return new Country { /* ... */ };
}
```

## 8. Приоритетные улучшения (рекомендуется начать с них)

1. ✅ **Использование параметризованных геометрических типов** в `GeoLocationService`
2. ✅ **Выделение общего метода** для пространственных запросов
3. ✅ **Разделение DatabaseWriterService** на более мелкие классы
4. ✅ **Выделение констант** для имен полей GeoJSON
5. ✅ **Выделение SQL-запросов** в отдельный класс

## 9. Дополнительные соображения

### 9.1. Мониторинг и метрики

Добавить метрики для:
- Время выполнения запросов
- Количество запросов в секунду
- Процент попаданий в кэш (если используется)
- Количество ошибок

### 9.2. Логирование

Использовать структурированное логирование:
```csharp
_logger.LogInformation(
    "Resolved geo-location: {Latitude}, {Longitude} -> {Country}, {Region}, {City}",
    latitude, longitude, country?.NameLatin, region?.NameLatin, city?.NameLatin);
```

### 9.3. Валидация входных данных

Вынести валидацию координат в отдельный метод:
```csharp
public static class CoordinateValidator
{
    public static bool IsValidLatitude(double latitude) => 
        latitude >= -90 && latitude <= 90;
    
    public static bool IsValidLongitude(double longitude) => 
        longitude >= -180 && longitude <= 180;
    
    public static void ValidateCoordinates(double latitude, double longitude)
    {
        if (!IsValidLatitude(latitude))
            throw new ArgumentOutOfRangeException(nameof(latitude));
        if (!IsValidLongitude(longitude))
            throw new ArgumentOutOfRangeException(nameof(longitude));
    }
}
```
