using GeoResolver.Models;

namespace GeoResolver.Services;

public class GeoLocationService : IGeoLocationService
{
    private readonly IDatabaseService _databaseService;

    public GeoLocationService(IDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    public async Task<GeoLocationResponse?> GetLocationAsync(double latitude, double longitude, CancellationToken cancellationToken = default)
    {
        var countryTask = _databaseService.FindCountryByPointAsync(latitude, longitude, cancellationToken);
        var regionTask = _databaseService.FindRegionByPointAsync(latitude, longitude, cancellationToken);
        var cityTask = _databaseService.FindCityByPointAsync(latitude, longitude, cancellationToken);
        var timezoneTask = _databaseService.GetTimezoneOffsetAsync(latitude, longitude, cancellationToken);

        await Task.WhenAll(countryTask, regionTask, cityTask, timezoneTask);

        var country = await countryTask;
        if (country == null)
        {
            return null;
        }

        var region = await regionTask;
        var city = await cityTask;
        var timezone = await timezoneTask;

        return new GeoLocationResponse
        {
            CountryIsoAlpha2Code = country.IsoAlpha2Code,
            CountryNameLatin = country.NameLatin,
            RegionIdentifier = region?.Identifier,
            RegionNameLatin = region?.NameLatin,
            CityIdentifier = city?.Identifier,
            CityNameLatin = city?.NameLatin,
            TimezoneRawOffsetSeconds = timezone?.RawOffset ?? 0,
            TimezoneDstOffsetSeconds = timezone?.DstOffset ?? 0
        };
    }
}

