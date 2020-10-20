package util.web;

/**
 * @author Kevin Chen
 */
public class GdeltFileNameFormatter
{

	private final GdeltConfiguration gdeltConfiguration;

    public GdeltFileNameFormatter(GdeltConfiguration gdeltConfiguration)
    {
        this.gdeltConfiguration = gdeltConfiguration;
    }

    public static String padToTwoDigits(int i)
    {
        return String.format("%02d", i);
    }

    public String formatGdeltTime(int year, int month, int dayOfMonth, int hour, int minute)
    {
        return String.format(
            "%d%s%s%s%s00",
            year,
            padToTwoDigits(month),
            padToTwoDigits(dayOfMonth),
            padToTwoDigits(hour),
            padToTwoDigits(minute)
        );
    }
    
    public String formatGdeltTime(int year, int month, int dayOfMonth)
    {
        return String.format(
            "%d%s%s",
            year,
            padToTwoDigits(month),
            padToTwoDigits(dayOfMonth)
        );
    }

    public String formatGdeltUrl(int year, int month, int dayOfMonth, int hour, int minute)
    {
        return String.format(
            "%s/%s.export.CSV.zip",
            gdeltConfiguration.getGdeltV2URL(),
            formatGdeltTime(year, month, dayOfMonth, hour, minute)
        );
    }

    public String formatGdeltCsvFilename(int year, int month, int dayOfMonth, int hour, int minute)
    {
        return String.format("%s.export.CSV", formatGdeltTime(year, month, dayOfMonth, hour, minute));
    }

    /**
     * local dir
     * @param year
     * @param month
     * @param dayOfMonth
     * @return
     */
    public String formatGdeltCsvFilename(int year, int month, int dayOfMonth)
    {
        return String.format("%s.export.CSV", formatGdeltTime(year, month, dayOfMonth));
    }
    
    public String formatGdeltZippedCsvFilename(int year, int month, int dayOfMonth, int hour, int minute)
    {
        return String.format("%s.zip", formatGdeltCsvFilename(year, month, dayOfMonth, hour, minute));
    }
    
    public String formatGdeltZippedCsvFilename(int year, int month, int dayOfMonth)
    {
        return String.format("%s.zip", formatGdeltCsvFilename(year, month, dayOfMonth));
    }
    
}
