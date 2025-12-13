namespace GeoResolver.Options;

public sealed class DataUpdateOptions
{
    public const string SectionName = "DataUpdate";

    public bool ForceUpdateOnStart { get; set; } = false;
    public int IntervalDays { get; set; } = 365;
}

