# GeoResolver

Микросервис для определения страны, региона, города и смещения временного пояса по геокоординатам.

## Описание

GeoResolver - это Minimal API на .NET 10, который предоставляет REST API для геолокации на основе координат. Сервис использует открытые источники данных и PostgreSQL с расширением PostGIS для хранения геопространственных данных.

## Основные возможности

- Определение страны по координатам (ISO 3166-1 alpha-2)
- Определение региона (admin level 1) с идентификаторами
- Определение города с идентификаторами
- Определение смещения временного пояса (RawOffset и DstOffset в секундах)
- Автоматическое обновление данных из открытых источников
- Распределенные блокировки для безопасного обновления данных в кластере
- Health check endpoint для мониторинга

## Требования

- .NET 10 SDK
- PostgreSQL 12+ с расширением PostGIS
- Docker (для контейнеризации)

## Установка и настройка

### 1. Создание базы данных

**При использовании Docker Compose:** база данных создается автоматически при первом запуске. Скрипты с префиксом `docker-` выполняются автоматически через `docker-entrypoint-initdb.d` в следующем порядке:
1. `docker-01-create-user.sql` - создает пользователя PostgreSQL (база данных создается через переменную окружения `POSTGRES_DB`)
2. `docker-02-enable-postgis.sql` - включает расширение PostGIS в базе данных
3. `docker-03-create-tables.sql` - создает таблицы, индексы и другие объекты

Все скрипты идемпотентны и безопасны для повторного запуска.

**При ручной установке PostgreSQL:** используйте скрипт для автоматической настройки:

```bash
chmod +x scripts/setup-database.sh
./scripts/setup-database.sh
```

Или выполните скрипты с префиксом `manual-` вручную по порядку:

```bash
# 1. Создать пользователя и базу данных
psql -U postgres -f scripts/manual-01-create-user-and-database.sql

# 2. Включить PostGIS
psql -U postgres -d georesolver -f scripts/manual-02-enable-postgis.sql

# 3. Создать таблицы и индексы
psql -U postgres -d georesolver -f scripts/manual-03-create-tables.sql

# Для удаления таблиц (опционально):
# psql -U postgres -d georesolver -f scripts/manual-04-drop-tables.sql
```

### 2. Настройка подключения к базе данных

Настройте параметры подключения в `appsettings.json` или через переменные окружения:

```json
{
  "Database": {
    "Host": "localhost",
    "Port": "5432",
    "Name": "georesolver",
    "Username": "georesolver",
    "Password": "georesolver_password"
  }
}
```

Или через переменные окружения:

```bash
export Database__Host=localhost
export Database__Port=5432
export Database__Name=georesolver
export Database__Username=georesolver
export Database__Password=your_password
```

### 3. Инициализация данных

Данные обновляются вручную с помощью отдельного консольного приложения `GeoResolver.DataUpdater`. 

**Локальный запуск загрузки данных:**

```bash
cd GeoResolver.DataUpdater
dotnet run
```

Или из корня решения:

```bash
dotnet run --project GeoResolver.DataUpdater
```

Приложение:
- Загружает данные о странах, регионах и городах из открытых источников
- Очищает существующие данные перед загрузкой новых
- Использует распределенные блокировки для предотвращения одновременного запуска нескольких процессов
- Обновляет время последнего обновления в базе данных

**Настройка подключения к базе данных** в `GeoResolver.DataUpdater/appsettings.json`:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Host=localhost;Port=5432;Database=georesolver;Username=georesolver;Password=georesolver_password"
  }
}
```

Или через переменную окружения:
```bash
export ConnectionStrings__DefaultConnection="Host=localhost;Port=5432;Database=georesolver;Username=georesolver;Password=your_password"
```

## Запуск

### Локальный запуск

```bash
dotnet run
```

Сервис будет доступен по адресу: `http://localhost:5000`

### Запуск в Docker

#### Использование Docker Compose (рекомендуется)

Самый простой способ запустить весь стек приложения (PostgreSQL + GeoResolver):

```bash
docker compose up -d
```

Эта команда:
- Соберёт образ GeoResolver
- Запустит PostgreSQL с PostGIS и автоматически выполнит инициализацию БД
- Запустит GeoResolver и подключит его к базе данных
- Настроит все необходимые зависимости

Сервис будет доступен по адресу: `http://localhost:8080`

Для просмотра логов:

```bash
docker compose logs -f geo-resolver
```

Для остановки:

```bash
docker compose down
```

Для остановки с удалением данных:

```bash
docker compose down -v
```

#### Ручная сборка Docker образа

Если нужно собрать только образ приложения:

```bash
docker build -t geo-resolver .
```

Запустите контейнер (требуется отдельный PostgreSQL):

```bash
docker run -d \
  -p 8080:8080 \
  -e Database__Host=your_postgres_host \
  -e Database__Password=your_password \
  --name geo-resolver \
  geo-resolver
```

## API

### GET /api/geolocation

Возвращает информацию о местоположении по координатам.

**Параметры запроса:**

- `latitude` (required, double) - широта в диапазоне от -90 до 90
- `longitude` (required, double) - долгота в диапазоне от -180 до 180

**Пример запроса:**

```http
GET /api/geolocation?latitude=55.7558&longitude=37.6173
```

**Пример ответа:**

```json
{
  "countryIsoAlpha2Code": "RU",
  "countryNameLatin": "Russia",
  "regionIdentifier": null,
  "regionNameLatin": null,
  "cityIdentifier": null,
  "cityNameLatin": null,
  "timezoneRawOffsetSeconds": 10800,
  "timezoneDstOffsetSeconds": 0,
  "timezoneTotalOffsetSeconds": 10800
}
```

**Коды ответов:**

- `200 OK` - успешный ответ
- `400 Bad Request` - неверные параметры запроса
- `404 Not Found` - местоположение не найдено

### GET /health

Health check endpoint для проверки состояния сервиса и подключения к базе данных.

**Пример запроса:**

```http
GET /health
```

**Пример ответа:**

```json
{
  "status": "Healthy",
  "totalDuration": "00:00:00.0123456"
}
```

## Тестирование

Используйте файл `GeoResolver.http` для тестирования API в IDE с поддержкой HTTP файлов (например, Rider, Visual Studio Code с расширением REST Client).

Примеры запросов:

```http
### Get GeoLocation - Moscow
GET http://localhost:5000/api/geolocation?latitude=55.7558&longitude=37.6173

### Get GeoLocation - New York
GET http://localhost:5000/api/geolocation?latitude=40.7128&longitude=-74.0060

### Health Check
GET http://localhost:5000/health
```

## Источники данных

Сервис использует следующие открытые источники данных:

1. **Natural Earth Data** - для границ стран с ISO кодами
2. **Timezone Boundary Builder** - для границ временных поясов (требует ручной загрузки)

Примечание: Регионы и города в текущей версии не загружаются автоматически, так как требуют дополнительной обработки данных из GeoNames или Natural Earth Admin 1.

## Кэширование

**Анализ необходимости кэширования:**

Для данного сервиса кэширование на уровне приложения **не критично** по следующим причинам:

1. **PostgreSQL + PostGIS уже оптимизированы** для пространственных запросов с использованием GIST индексов
2. **Запросы выполняются быстро** благодаря индексам на геометрических полях
3. **Данные обновляются редко** (раз в год), поэтому кэш не даст значительного выигрыша
4. **Простота архитектуры** - отсутствие кэша упрощает развертывание и поддержку

Однако, если требуется высокая нагрузка (тысячи запросов в секунду), можно добавить:
- Кэширование в памяти (IMemoryCache) с TTL
- Кэширование на уровне HTTP (ResponseCache)
- Redis для распределенного кэширования

## Health Check

Health check endpoint (`/health`) **рекомендуется и реализован** по следующим причинам:

1. **Мониторинг** - позволяет оркестраторам (Kubernetes, Docker Swarm) проверять состояние сервиса
2. **Балансировка нагрузки** - позволяет исключать нездоровые инстансы из пула
3. **Отладка** - быстрая проверка доступности базы данных
4. **Автоматическое восстановление** - интеграция с системами автоматического перезапуска

Endpoint проверяет:
- Доступность базы данных
- Возможность выполнения запросов к PostgreSQL

## Архитектура

```
┌─────────────────┐
│   HTTP Client   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Minimal API    │         ┌──────────────────────┐
│  (GeoResolver)  │         │ GeoResolver.DataUpdater│
│                 │         │   (Console App)        │
└────────┬────────┘         └──────────┬───────────┘
         │                             │
         ▼                             ▼
┌─────────────────┐         ┌──────────────────┐
│GeoLocationService│         │   DataLoader     │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         ▼                           ▼
┌─────────────────┐         ┌──────────────────────┐
│GeoLocationService│        │ DatabaseWriterService│
└────────┬────────┘         └────────┬─────────┘
         │                           │
         └──────────┬────────────────┘
                    ▼
         ┌──────────────────┐
         │   PostgreSQL     │
         │   + PostGIS      │
         └──────────────────┘
```

## Распределенные блокировки

Приложение для обновления данных (`GeoResolver.DataUpdater`) использует библиотеку `DistributedLock.Postgres` для реализации распределенных блокировок через PostgreSQL advisory locks. Это позволяет запускать несколько процессов обновления данных одновременно без конфликтов.

Механизм работы:
1. При запуске `GeoResolver.DataUpdater` процесс пытается получить распределенную блокировку через PostgreSQL advisory locks
2. Если блокировка уже захвачена другим процессом, текущий процесс завершается с предупреждением
3. После успешной загрузки данных обновляется запись в таблице `last_update`

## Разработка

### Структура проекта

```
GeoResolver/
├── Models/
│   ├── GeoLocationResponse.cs    # Модель ответа API
│   └── DatabaseModels.cs          # Модели базы данных
├── Services/
│   ├── GeoLocationService.cs      # Логика геолокации и работа с БД (чтение)
│   └── DatabaseHealthCheck.cs     # Health check
GeoResolver.DataUpdater/
├── Models/
│   └── DatabaseModels.cs          # Модели базы данных (для записи)
├── Services/
│   ├── DatabaseWriterService.cs   # Сервис для записи в БД
│   └── DataLoaders/
│       ├── DataLoader.cs          # Загрузка данных из внешних источников
│       └── IDataLoader.cs
└── Program.cs                     # Консольное приложение для обновления данных
├── scripts/
│   ├── docker-01-create-user.sql          # Автоматически: создание пользователя (для Docker)
│   ├── docker-02-enable-postgis.sql       # Автоматически: включение PostGIS (для Docker)
│   ├── docker-03-create-tables.sql        # Автоматически: создание таблиц (для Docker)
│   ├── manual-01-create-user-and-database.sql  # Ручной запуск: создание пользователя и БД
│   ├── manual-02-enable-postgis.sql       # Ручной запуск: включение PostGIS
│   ├── manual-03-create-tables.sql        # Ручной запуск: создание таблиц и индексов
│   ├── manual-04-drop-tables.sql          # Ручной запуск: удаление таблиц (для очистки)
│   └── setup-database.sh                  # Скрипт для полной настройки БД вручную
├── Program.cs                     # Точка входа
├── appsettings.json               # Конфигурация
├── Dockerfile                     # Docker образ
└── README.md                      # Документация
```

### Сборка

```bash
dotnet build -c Release
```

### Публикация

Локальная публикация:

```bash
dotnet publish -c Release
```

Публикация для конкретной платформы:

```bash
# Linux x64
dotnet publish -c Release -r linux-x64 --self-contained

# Linux ARM64 (совместимо с M1 Mac)
dotnet publish -c Release -r linux-arm64 --self-contained

# Windows
dotnet publish -c Release -r win-x64 --self-contained

# macOS
dotnet publish -c Release -r osx-x64 --self-contained
```

**Docker сборка:**

Dockerfile использует легковесный Ubuntu образ (jammy) с non-root пользователем, как рекомендует Microsoft. Образ поддерживает различные платформы благодаря .NET runtime.

## Лицензия

Проект использует открытые источники данных и библиотеки с соответствующими лицензиями.

## Поддержка

При возникновении проблем проверьте:
1. Доступность PostgreSQL с расширением PostGIS
2. Правильность настроек подключения к БД
3. Логи приложения для диагностики ошибок

