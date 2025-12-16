namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Service for transliterating text from various scripts (Cyrillic, Arabic, etc.) to Latin script.
/// </summary>
public interface ITransliterationService
{
	/// <summary>
	///     Transliterates text to Latin script using ICU transliteration.
	///     Returns null if transliteration fails or text is empty.
	/// </summary>
	/// <param name="text">Text to transliterate (can be in any script: Cyrillic, Arabic, etc.)</param>
	/// <returns>Transliterated text in Latin script, or null if transliteration failed</returns>
	string? TransliterateToLatin(string text);

	/// <summary>
	///     Checks if the string contains at least one Latin character (A-Z, a-z).
	/// </summary>
	/// <param name="text">Text to check</param>
	/// <returns>True if text contains at least one Latin character</returns>
	bool ContainsLatinCharacters(string text);

	/// <summary>
	///     Cleans text by replacing all non-ASCII characters with spaces and trimming the result.
	/// </summary>
	/// <param name="text">Text to clean</param>
	/// <returns>Cleaned text with non-ASCII characters replaced by spaces and trimmed</returns>
	string CleanTransliterationResult(string text);
}
