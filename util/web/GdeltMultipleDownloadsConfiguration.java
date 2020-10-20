package util.web;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * @author Kevin Chen
 */
public class GdeltMultipleDownloadsConfiguration
{
    private GdeltWebFetchApi gdeltApi;
    private File directory = GdeltDefaultDirectoryFileFactory.getDefaultDirectory();
    private boolean unzip = true;
    private boolean deleteZipFile = false;
    private LocalDateTime since;
    private LocalDateTime until;

    public GdeltMultipleDownloadsConfiguration(GdeltWebFetchApi gdeltApi, LocalDateTime since)
    {
        this(gdeltApi, since, LocalDateTime.now());
    }

    public GdeltMultipleDownloadsConfiguration(GdeltWebFetchApi gdeltApi, LocalDateTime since, LocalDateTime until)
    {
        this.gdeltApi = gdeltApi;
        this.since = roundDown(removeSecondsAndNanos(since));
        this.until = roundDown(removeSecondsAndNanos(until));
    }

    public GdeltMultipleDownloadsConfiguration toDirectory(File directory)
    {
        this.directory = directory;
        return this;
    }

    public GdeltMultipleDownloadsConfiguration unzip(boolean unzip)
    {
        this.unzip = unzip;
        return this;
    }

    public GdeltMultipleDownloadsConfiguration deleteZipFile(boolean deleteZipFile)
    {
        this.deleteZipFile = deleteZipFile;
        return this;
    }

    public GdeltMultipleDownloadsConfiguration until(LocalDateTime until)
    {
        this.until = until;
        return this;
    }

    public void execute()
    {
        if (until == null)
        {
            // we cannot round up to next interval of 15, since it may not have been released yet
            until = roundDown(LocalDateTime.now());
        }

        LocalDateTime time = since;
        while (!time.isAfter(until))
        {
            gdeltApi.tryDownloadUpdate(directory, unzip, deleteZipFile, time.getYear(), time.getMonth().getValue(), time.getDayOfMonth(), time.getHour(), time.getMinute());
            time = time.plusMinutes(15);
        }
    }

    private LocalDateTime roundDown(LocalDateTime time)
    {
        int minute = time.getMinute();
        return time.minusMinutes(minute % 15);
    }

    private LocalDateTime removeSecondsAndNanos(LocalDateTime time)
    {
        Objects.requireNonNull(time);
        return time.minusSeconds(time.getSecond()).minusNanos(time.getNano());
    }
}
