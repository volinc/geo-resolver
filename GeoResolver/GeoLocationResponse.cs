namespace GeoResolver;

public sealed record GeoLocationResponse
{
    /// <summary>
    /// ISO 3166-1 alpha-2 country code (e.g., "US", "RU", "GB")
    /// Required if the point is within a country's territory
    /// </summary>
    public string? CountryIsoAlpha2Code { get; init; }

    /// <summary>
    /// ISO 3166-1 alpha-3 country code (e.g., "USA", "RUS", "GBR")
    /// </summary>
    public string? CountryIsoAlpha3Code { get; init; }

    /// <summary>
    /// Country name in translated into English
    /// </summary>
    public string? CountryNameLatin { get; init; }

    /// <summary>
    /// Region identifier ISO 3166-2 (e.g. "RU-MOW")
    /// </summary>
    public string? RegionIdentifier { get; init; }

    /// <summary>
    /// Region name in Latin transliteration (invariant) or translation into English
    /// </summary>
    public string? RegionNameLatin { get; init; }

    /// <summary>
    /// City identifier OSM ID (unsafe - may change with the next data update)
    /// </summary>
    public string? CityIdentifier { get; init; }

    /// <summary>
    /// City name in Latin transliteration (invariant) or translation into English
    /// </summary>
    public string? CityNameLatin { get; init; }

    /// <summary>
    /// Raw timezone offset from UTC in seconds (without DST)
    /// </summary>
    public int TimezoneRawOffsetSeconds { get; init; }

    /// <summary>
    /// Daylight Saving Time offset from UTC in seconds
    /// </summary>
    public int TimezoneDstOffsetSeconds { get; init; }

    /// <summary>
    /// Total timezone offset from UTC in seconds (RawOffset + DstOffset)
    /// </summary>
    public int TimezoneTotalOffsetSeconds => TimezoneRawOffsetSeconds + TimezoneDstOffsetSeconds;
}

