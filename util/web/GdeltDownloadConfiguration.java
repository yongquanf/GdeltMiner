package util.web;

import java.io.File;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

/**
 * @author Kevin Chen
 */
public class GdeltDownloadConfiguration
{
    private GdeltWebFetchApi gdeltApi;
    private File directory = GdeltDefaultDirectoryFileFactory.getDefaultDirectory();
    private boolean unzip = true;
    private boolean deleteZipFile = false;
    private OffsetDateTime offsetDateTime;
    private LocalDateTime localDateTime;

    public GdeltDownloadConfiguration(GdeltWebFetchApi gdeltApi)
    {
        this.gdeltApi = gdeltApi;
    }

    public GdeltDownloadConfiguration toDirectory(File directory)
    {
        this.directory = directory;
        return this;
    }

    public GdeltDownloadConfiguration unzip(boolean unzip)
    {
        this.unzip = unzip;
        return this;
    }

    public GdeltDownloadConfiguration deleteZipFile(boolean deleteZipFile)
    {
        this.deleteZipFile = deleteZipFile;
        return this;
    }

    public GdeltDownloadConfiguration atTime(OffsetDateTime offsetDateTime)
    {
        this.offsetDateTime = offsetDateTime;
        return this;
    }

    public GdeltDownloadConfiguration atTime(LocalDateTime localDateTime)
    {
        this.localDateTime = localDateTime;
        return this;
    }

    public File execute()
    {
        int year;
        int month;
        int dayOfMonth;
        int hour;
        int minute;

        if (offsetDateTime != null)
        {
            validateOffsetDateTime();
            year = offsetDateTime.getYear();
            month = offsetDateTime.getMonth().getValue();
            dayOfMonth = offsetDateTime.getDayOfMonth();
            hour = offsetDateTime.getHour();
            minute = offsetDateTime.getMinute();
        }
        else if (localDateTime != null)
        {
            validateLocalDateTime();
            year = localDateTime.getYear();
            month = localDateTime.getMonth().getValue();
            dayOfMonth = localDateTime.getDayOfMonth();
            hour = localDateTime.getHour();
            minute = localDateTime.getMinute();
        }
        else
        {
            throw new IllegalArgumentException("Must specify a dateTime.");
        }

        return gdeltApi.downloadUpdate(directory, unzip, deleteZipFile, year, month, dayOfMonth, hour, minute);
    }

    private void validateOffsetDateTime()
    {
        if (offsetDateTime.getMinute() % 15 != 0)
        {
            throw new IllegalArgumentException("Given dateTime must be multiple of 15 minutes. Received: " + offsetDateTime);
        }

        if (offsetDateTime.isAfter(OffsetDateTime.now()))
        {
            throw new IllegalArgumentException("Given dateTime must be before now. Received: " + offsetDateTime);
        }
    }

    private void validateLocalDateTime()
    {
        if (localDateTime.getMinute() % 15 != 0)
        {
            throw new IllegalArgumentException("Given dateTime must be multiple of 15 minutes. Received: " + localDateTime);
        }

        if (localDateTime.isAfter(LocalDateTime.now()))
        {
            throw new IllegalArgumentException("Given dateTime must be before now. Received: " + localDateTime);
        }
    }
}
