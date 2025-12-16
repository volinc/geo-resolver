using ICU4N.Text;
using Microsoft.Extensions.Logging;

namespace GeoResolver.DataUpdater.Services;

/// <summary>
///     Service for transliterating text from various scripts (Cyrillic, Arabic, etc.) to Latin script using ICU.
///     Uses Google Maps-style transliteration for Cyrillic, standard ICU rules for other scripts.
/// </summary>
public sealed class TransliterationService : ITransliterationService
{
	private readonly ILogger<TransliterationService>? _logger;
	private Transliterator? _cyrillicTransliterator;

	public TransliterationService(ILogger<TransliterationService>? logger = null)
	{
		_logger = logger;
		InitializeCyrillicTransliterator();
	}

	/// <summary>
	///     Initializes custom Cyrillic transliterator with Google Maps-style rules.
	/// </summary>
	private void InitializeCyrillicTransliterator()
	{
		try
		{
			// Google Maps-style transliteration rules for Cyrillic
			// Based on BGN/PCGN transliteration system used by Google Maps
			var rules = @"
                # Normalize first
                :: NFD;
                
                # Basic consonants (lowercase)
                ж > zh;
                х > kh;
                ц > ts;
                ч > ch;
                ш > sh;
                щ > shch;
                
                # Vowels and special letters (lowercase)
                ю > yu;
                я > ya;
                ё > yo;
                э > e;
                й > y;
                ы > y;
                
                # Soft and hard signs - remove them completely (Google Maps style)
                ь > ;
                ъ > ;
                
                # Uppercase versions
                Ж > Zh;
                Х > Kh;
                Ц > Ts;
                Ч > Ch;
                Ш > Sh;
                Щ > Shch;
                Ю > Yu;
                Я > Ya;
                Ё > Yo;
                Э > E;
                Й > Y;
                Ы > Y;
                Ь > ;
                Ъ > ;
                
                # Fallback for any remaining Cyrillic characters
                :: Cyrillic-Latin;
                
                # Remove diacritics and normalize
                :: NFD;
                :: [:Nonspacing Mark:] Remove;
                :: NFC;
            ";

			_cyrillicTransliterator = Transliterator.CreateFromRules("CustomCyrillicGoogle", rules, Transliterator.Forward);
		}
		catch (Exception ex)
		{
			_logger?.LogWarning(ex, "Failed to create custom Cyrillic transliterator, will use standard rules");
			_cyrillicTransliterator = null;
		}
	}

	/// <summary>
	///     Checks if text contains Cyrillic characters.
	/// </summary>
	private bool ContainsCyrillic(string text)
	{
		return text.Any(c => c >= 0x0400 && c <= 0x04FF);
	}

	/// <summary>
	///     Transliterates text to Latin script using ICU transliteration.
	///     Uses Google Maps-style rules for Cyrillic, standard ICU rules for other scripts.
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
			string? result = null;

			// For Cyrillic text, use custom Google Maps-style transliterator
			if (ContainsCyrillic(text) && _cyrillicTransliterator != null)
			{
				try
				{
					result = _cyrillicTransliterator.Transliterate(text);
					if (!string.IsNullOrWhiteSpace(result) && result != text)
					{
						// Clean up: remove any remaining special characters, keep only ASCII letters, numbers, spaces, hyphens, dots
						result = CleanTransliterationResult(result);
						if (!string.IsNullOrWhiteSpace(result) && 
						    result.Any(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')))
						{
							return result;
						}
					}
				}
				catch (Exception ex)
				{
					_logger?.LogDebug(ex, "Custom Cyrillic transliteration failed for text: '{Text}', trying standard rules", text);
				}
			}

			// For non-Cyrillic or if custom transliterator failed, use standard ICU rules
			var transliterationRules = new[]
			{
				"Any-Latin; Latin-ASCII", // Most general: any script to Latin, then to ASCII
				"Any-Latin", // Fallback: any script to Latin (may have diacritics)
			};

			foreach (var rule in transliterationRules)
			{
				try
				{
					var transliterator = Transliterator.GetInstance(rule);
					
					if (transliterator == null)
						continue;
					
					var ruleResult = transliterator.Transliterate(text);
					
					if (!string.IsNullOrWhiteSpace(ruleResult) && ruleResult != text)
					{
						// Clean up the result
						ruleResult = CleanTransliterationResult(ruleResult);
						
						if (!string.IsNullOrWhiteSpace(ruleResult) && 
						    ruleResult.Any(c => (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')))
						{
							return ruleResult;
						}
					}
				}
				catch (Exception ex)
				{
					_logger?.LogDebug(ex, "Transliteration rule '{Rule}' failed for text: '{Text}'", rule, text);
					continue;
				}
			}

			_logger?.LogWarning("All transliteration rules failed for text: '{Text}'", text);
			return null;
		}
		catch (Exception ex)
		{
			_logger?.LogError(ex, "Transliteration failed for text: '{Text}'", text);
			return null;
		}
	}

	/// <summary>
	///     Cleans transliteration result by removing special characters, keeping only ASCII letters, numbers, spaces, hyphens, dots.
	/// </summary>
	private string CleanTransliterationResult(string result)
	{
		var cleaned = new System.Text.StringBuilder();
		foreach (var c in result)
		{
			if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || 
			    (c >= '0' && c <= '9') || char.IsWhiteSpace(c) || 
			    c == '-' || c == '.')
			{
				cleaned.Append(c);
			}
			// Skip apostrophes, quotes, and other special characters
		}
		return cleaned.ToString().Trim();
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
