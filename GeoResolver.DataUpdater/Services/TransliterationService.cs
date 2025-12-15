using ICU4N.Text;

namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Service for transliterating text from various scripts (Cyrillic, Arabic, etc.) to Latin script using ICU.
/// </summary>
public sealed class TransliterationService : ITransliterationService
{
	/// <summary>
	///     Transliterates text to Latin script using ICU transliteration.
	///     Returns null if transliteration fails or text is empty.
	/// </summary>
	/// <param name="text">Text to transliterate (can be in any script: Cyrillic, Arabic, etc.)</param>
	/// <returns>Transliterated text in Latin script, or null if transliteration failed</returns>
	public string? TransliterateToLatin(string text)
	{
		if (string.IsNullOrWhiteSpace(text))
			return null;

		try
		{
			// Try common transliteration rules
			// ICU supports various transliteration IDs like "Cyrillic-Latin", "Any-Latin", etc.
			// "Any-Latin; Latin-ASCII" attempts to transliterate any script to Latin, then to ASCII
			var transliterator = Transliterator.GetInstance("Any-Latin; Latin-ASCII");
			return transliterator.Transliterate(text);
		}
		catch
		{
			// If ICU transliteration fails, return null
			return null;
		}
	}

	/// <summary>
	///     Checks if the string contains at least one Latin character (A-Z, a-z).
	/// </summary>
	/// <param name="text">Text to check</param>
	/// <returns>True if text contains at least one Latin character</returns>
	public bool ContainsLatinCharacters(string text)
	{
		// Check if string contains at least one Latin character (A-Z, a-z)
		// This helps filter out names in Cyrillic, Arabic, Chinese, etc.
		return text.Any(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'));
	}
}
