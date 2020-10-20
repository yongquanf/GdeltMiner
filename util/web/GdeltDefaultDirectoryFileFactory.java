package util.web;

import static util.web.GdeltFileNameFormatter.padToTwoDigits;

import java.io.File;
import java.time.LocalDateTime;

/**
 * @author Kevin Chen
 */
public class GdeltDefaultDirectoryFileFactory
{
	public static File getDefaultDirectory()
    {
        return new File(System.getProperty("user.home") + File.separator + "gdelt");
    }

    public static File getDirectory(File parentDestinationDir, String url)
    {
        LocalDateTime time = GdeltUrlTimeParser.parseTimeFromUrl(url);
        return getDirectory(parentDestinationDir, time.getYear(), time.getMonth().getValue(), time.getDayOfMonth());
    }

    public static File getDirectory(File parentDestinationDir, int year, int month, int dayOfMonth)
    {
        if (parentDestinationDir == null)
        {
            throw new IllegalArgumentException("Cannot provide null directory File");
        }
        return new File(parentDestinationDir.getAbsolutePath() + File.separator +
            year + File.separator +
            padToTwoDigits(month) + File.separator +
            padToTwoDigits(dayOfMonth));
    }
    
    
    
}
