using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Logging;

public class FileLoggerProvider : ILoggerProvider
{
	private readonly ConcurrentDictionary<string, FileLogger> _loggers = new();
	private readonly LogLevel _minimumLevel;
	private readonly StreamWriter _streamWriter;

	public FileLoggerProvider(string filePath, LogLevel minimumLevel = LogLevel.Information)
	{
		_minimumLevel = minimumLevel;
		var directory = Path.GetDirectoryName(filePath);
		if (!string.IsNullOrEmpty(directory)) Directory.CreateDirectory(directory);

		_streamWriter = new StreamWriter(filePath, true)
		{
			AutoFlush = true
		};
	}

	public ILogger CreateLogger(string categoryName)
	{
		return _loggers.GetOrAdd(categoryName, name => new FileLogger(name, _streamWriter, _minimumLevel));
	}

	public void Dispose()
	{
		_streamWriter?.Dispose();
		_loggers.Clear();
	}
}

internal class FileLogger : ILogger
{
	private readonly string _categoryName;
	private readonly LogLevel _minimumLevel;
	private readonly StreamWriter _streamWriter;

	public FileLogger(string categoryName, StreamWriter streamWriter, LogLevel minimumLevel)
	{
		_categoryName = categoryName;
		_streamWriter = streamWriter;
		_minimumLevel = minimumLevel;
	}

	public IDisposable? BeginScope<TState>(TState state) where TState : notnull
	{
		return null;
	}

	public bool IsEnabled(LogLevel logLevel)
	{
		return logLevel >= _minimumLevel;
	}

	public void Log<TState>(
		LogLevel logLevel,
		EventId eventId,
		TState state,
		Exception? exception,
		Func<TState, Exception?, string> formatter)
	{
		if (!IsEnabled(logLevel)) return;

		var message = formatter(state, exception);
		var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
		var logLevelString = logLevel.ToString().ToUpperInvariant().PadRight(5);

		var logMessage = $"{timestamp} [{logLevelString}] {_categoryName}: {message}";

		if (exception != null) logMessage += Environment.NewLine + exception;

		lock (_streamWriter)
		{
			_streamWriter.WriteLine(logMessage);
		}
	}
}