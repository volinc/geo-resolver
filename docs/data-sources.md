# Data sources

GeoResolver uses a combination of open geospatial datasets for countries, regions,
cities and timezones. This document describes the sources and how the data is filtered.

## Overview

1. **Countries** – Natural Earth Data 10m Admin 0 Countries (Shapefile)
2. **Regions** – Natural Earth Data 10m Admin 1 States/Provinces (Shapefile)
3. **Cities** – OpenStreetMap via Geofabrik (Shapefile `gis_osm_places_a_free_1.shp`)
4. **Timezones** – Timezone Boundary Builder (GeoJSON)

Official project websites:

- Natural Earth – `https://www.naturalearthdata.com/`
- Geofabrik – `https://www.geofabrik.de/`
- Timezone Boundary Builder – `https://github.com/evansiroky/timezone-boundary-builder`

## Why cities come from OSM / Geofabrik

Natural Earth provides **point** data for populated places (`POINT` geometries),
while GeoResolver needs **polygon** boundaries (`MULTIPOLYGON`) to answer
"point in polygon" queries efficiently and precisely.

Geofabrik publishes OSM data with polygon geometries for cities in Shapefile format
(`gis_osm_places_a_free_1.shp`), which allows the service to use
`ST_Contains(geometry, point)` in PostgreSQL + PostGIS.

Additional advantages:

- OSM data is updated regularly.
- Geofabrik provides country‑ and region‑specific extracts which can be downloaded
  programmatically.

## City filtering

Raw OSM data includes many small settlements (villages, hamlets, suburbs, etc.).
To keep the database compact and queries fast, the updater imports **only significant
places** according to feature class and population.

### Included

A city polygon is imported if **any** of the following is true:

- `fclass = 'city'` – official cities
- `fclass = 'town'` – large towns / urban‑type settlements
- `fclass = 'national_capital'` – national capitals
- `population >= 10000` – settlements with population ≥ 10,000

### Excluded

The following feature classes are excluded:

- `fclass = 'village'` – villages
- `fclass = 'hamlet'` – hamlets
- `fclass = 'suburb'` – suburbs / city districts
- `fclass = 'neighbourhood'` – neighbourhoods / micro‑districts
- and other small settlement types

Filtering is applied at the **GDAL/OGR** stage when converting from Shapefile to
GeoJSON using an `ogr2ogr` `-where` clause. This avoids processing and importing
unnecessary records.

### Practical effect

For large countries like Russia, raw OSM may contain ~130,000 populated places.
After filtering, the importer keeps only about **1,000–2,000** significant cities
and large towns, which roughly matches official statistics (~1,125 cities as of 2021).

This significantly reduces database size and improves query performance while still
covering all major population centers.

## Timezones

Timezone polygons come from **Timezone Boundary Builder** in GeoJSON format.
The `GeoResolver` API uses NodaTime with the built‑in TZDB database to calculate
raw and DST offsets for the resolved timezone ID.

If no timezone polygon is found for a point, the service falls back to a simple
approximation based on longitude (each 15 degrees ≈ 1 hour), so timezone offsets
are always present in the response.
