namespace GeoResolver.DataUpdater.Services.Shapefile;

public interface IOsmCityShapefileLoader
{
	/// <summary>
	///     Loads cities for all countries listed in configuration section "CityLoader:Countries"
	///     (array of 2-letter ISO country codes, e.g. ["DE", "FR", "RU"]).
	/// </summary>
	Task LoadAllConfiguredCountriesAsync(CancellationToken cancellationToken = default);
}