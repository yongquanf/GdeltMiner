package util.web;

import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the time from a GDELT url.
 *
 * @author Kevin Chen
 */
public class GdeltUrlTimeParser
{
    static LocalDateTime parseTimeFromUrl(String url)
    {
        // http://data.gdeltproject.org/gdeltv2/20161201040000.export.CSV.zip

        // 20161201040000.export.CSV.zip
        String filename = url.substring(url.lastIndexOf("/") + 1);

        // 20161201040000
        filename = filename.substring(0, filename.indexOf("."));

        Pattern pattern = Pattern.compile("(\\d\\d\\d\\d)(\\d\\d)(\\d\\d)(.*)");
        Matcher matcher = pattern.matcher(filename);
        if (!matcher.find())
        {
            throw new IllegalArgumentException("Cannot parse time from invalid GDELT url: " + url);
        }

        int year = Integer.parseInt(matcher.group(1));
        int month = Integer.parseInt(matcher.group(2));
        int dayOfMonth = Integer.parseInt(matcher.group(3));
        return LocalDateTime.of(year, month, dayOfMonth, 0, 0);
    }
}
