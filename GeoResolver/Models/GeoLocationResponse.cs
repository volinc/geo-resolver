namespace GeoResolver.Models;

/// <summary>
/// Response model for geo-location lookup
/// </summary>
public record GeoLocationResponse
{
    /// <summary>
    /// ISO 3166-1 alpha-2 country code (e.g., "US", "RU", "GB")
    /// Required if the point is within a country's territory
    /// </summary>
    public required string CountryIsoAlpha2Code { get; init; }

    /// <summary>
    /// Country name in Latin transliteration (invariant)
    /// </summary>
    public required string CountryNameLatin { get; init; }

    /// <summary>
    /// Region identifier (e.g., FIPS or GeoNames ID)
    /// </summary>
    public string? RegionIdentifier { get; init; }

    /// <summary>
    /// Region name in Latin transliteration (invariant)
    /// Required if the point is within a region's territory
    /// </summary>
    public string? RegionNameLatin { get; init; }

    /// <summary>
    /// City identifier (e.g., FIPS or GeoNames ID)
    /// </summary>
    public string? CityIdentifier { get; init; }

    /// <summary>
    /// City name in Latin transliteration (invariant)
    /// Required if the point is within a city's territory
    /// </summary>
    public string? CityNameLatin { get; init; }

    /// <summary>
    /// Raw timezone offset from UTC in seconds (without DST)
    /// Similar to Google Timezone API RawOffset
    /// </summary>
    public int TimezoneRawOffsetSeconds { get; init; }

    /// <summary>
    /// Daylight Saving Time offset from UTC in seconds
    /// Similar to Google Timezone API DstOffset
    /// </summary>
    public int TimezoneDstOffsetSeconds { get; init; }

    /// <summary>
    /// Total timezone offset from UTC in seconds (RawOffset + DstOffset)
    /// </summary>
    public int TimezoneTotalOffsetSeconds => TimezoneRawOffsetSeconds + TimezoneDstOffsetSeconds;
}

