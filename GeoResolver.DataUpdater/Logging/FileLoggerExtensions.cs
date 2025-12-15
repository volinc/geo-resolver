using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Logging;

public static class FileLoggerExtensions
{
	public static void AddFile(this ILoggingBuilder builder, string filePath,
		LogLevel minimumLevel = LogLevel.Information)
	{
		builder.AddProvider(new FileLoggerProvider(filePath, minimumLevel));
	}
}