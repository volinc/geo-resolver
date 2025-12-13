namespace GeoResolver.DataUpdater.Services.DataLoaders;

public interface IDataLoader
{
    Task LoadAllDataAsync(CancellationToken cancellationToken = default);
}

