package util.web;

import util.csv.CsvProcessor;
import util.csv.GDELTReturnResult;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Kevin Chen
 */
public class GdeltWebFetchApi
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltWebFetchApi.class);

    private final HttpClient httpClient;

    private final GdeltConfiguration gdeltConfiguration;
    private final GdeltLastUpdateFetcher gdeltLastUpdateFetcher;
    private final GdeltLastUpdateDownloader gdeltLastUpdateDownloader;
    private final GdeltLastUpdateUnzipper gdeltLastUpdateUnzipper;
    private final GdeltFileNameFormatter gdeltFileNameFormatter;

    private final CsvProcessor csvProcessor;

    public GdeltWebFetchApi()
    {
        this(HttpClientBuilder.create().build());
    }

    /**
     * constructor
     * @param httpClient
     */
    public GdeltWebFetchApi(HttpClient httpClient)
    {
        this.httpClient = httpClient;
        this.gdeltConfiguration = new DefaultGdeltConfiguration();
        this.gdeltLastUpdateFetcher = new GdeltLastUpdateFetcher();
        this.gdeltLastUpdateDownloader = new GdeltLastUpdateDownloader();
        this.gdeltLastUpdateUnzipper = new GdeltLastUpdateUnzipper();
        this.gdeltFileNameFormatter = new GdeltFileNameFormatter(gdeltConfiguration);
        this.csvProcessor = new CsvProcessor();
    }

    public GdeltDownloadConfiguration download()
    {
        return new GdeltDownloadConfiguration(this);
    }
    
    public GdeltLastUpdateDownloadConfiguration downloadLastUpdate()
    {
        return new GdeltLastUpdateDownloadConfiguration(this);
    }
    
    public GdeltMultipleDownloadsConfiguration downloadAllBetween(LocalDateTime since, LocalDateTime until)
    {
        return new GdeltMultipleDownloadsConfiguration(this, since, until);
    }
    
    public GdeltMultipleDownloadsConfiguration downloadAllSince(LocalDateTime since)
    {
        return new GdeltMultipleDownloadsConfiguration(this, since);
    }

    public Optional<File> tryDownloadUpdate(File parentDestinationDir, boolean unzip, boolean deleteZip, int year, int month, int dayOfMonth, int hour, int minute)
    {
        try
        {
            return Optional.of(downloadUpdate(parentDestinationDir, unzip, deleteZip, year, month, dayOfMonth, hour, minute));
        }
        catch (Exception e)
        {
            logger.warn("Error downloading file for {}/{}/{}/{}/{}", year, month, dayOfMonth, hour, minute);
            logger.error("Error download file", e);
        }
        return Optional.empty();
    }

    /**
     * download
     * @param parentDestinationDir
     * @param unzip
     * @param deleteZip
     * @param year
     * @param month
     * @param dayOfMonth
     * @param hour
     * @param minute
     * @return
     */
    public File downloadUpdate(File parentDestinationDir, boolean unzip, boolean deleteZip, int year, int month, int dayOfMonth, int hour, int minute)
    {
        File destinationDir = GdeltDefaultDirectoryFileFactory.getDirectory(parentDestinationDir, year, month, dayOfMonth);
        if (destinationDir.exists())
        {
            File[] files = destinationDir.listFiles();
            if (files != null)
            {
                List<String> csvFileNames = Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .filter(n -> n.endsWith(".CSV"))
                    .collect(Collectors.toList());

                // we found the csv, just return it
                String csvFileName = gdeltFileNameFormatter.formatGdeltCsvFilename(year, month, dayOfMonth, hour, minute);
                if (csvFileNames.contains(csvFileName))
                {
                    logger.debug("Found CSV file for: {}", csvFileName);
                    return new File(destinationDir.getAbsolutePath() + File.separator + csvFileName);
                }

                List<String> zippedCsvFileNames = Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .filter(n -> n.endsWith(".CSV.zip"))
                    .collect(Collectors.toList());

                // we found the zipped csv, just unzip it and return the unzipped file
                String zippedCsvFileName = gdeltFileNameFormatter.formatGdeltZippedCsvFilename(year, month, dayOfMonth, hour, minute);
                if (zippedCsvFileNames.contains(zippedCsvFileName))
                {
                    logger.debug("Found zipped CSV file for: {}", zippedCsvFileName);
                    File zippedCsv = new File(destinationDir.getAbsolutePath() + File.separator + zippedCsvFileName);
                    return unzipCsv(zippedCsv, false);
                }
            }
        }
        else
        {
            destinationDir.mkdirs();
        }

        String url = gdeltFileNameFormatter.formatGdeltUrl(year, month, dayOfMonth, hour, minute);
        return downloadGdeltFile(url, destinationDir, unzip, deleteZip);
    }

    /**
     * download
     * @param parentDestinationDir
     * @param unzip
     * @param deleteZip
     * @return
     */
    public File downloadLastUpdate(File parentDestinationDir, boolean unzip, boolean deleteZip)
    {
        String lastUpdateUrl = gdeltLastUpdateFetcher.getGDELTLastUpdate(httpClient, gdeltConfiguration);
        File destinationDir = GdeltDefaultDirectoryFileFactory.getDirectory(parentDestinationDir, lastUpdateUrl);
        destinationDir.mkdirs();
        return downloadGdeltFile(lastUpdateUrl, destinationDir, unzip, deleteZip);
    }
    
    /**
     * download
     * @param url
     * @param destinationDir
     * @param unzip
     * @param deleteZip
     * @return
     */
    private File downloadGdeltFile(String url, File destinationDir, boolean unzip, boolean deleteZip)
    {
        File zippedCsvFile = gdeltLastUpdateDownloader.downloadGDELTZipFile(httpClient, destinationDir, url);

        if (unzip)
        {
            return unzipCsv(zippedCsvFile, deleteZip);
        }

        return zippedCsvFile;
    }

    private File unzipCsv(File zippedCsvFile, boolean deleteZip)
    {
        return gdeltLastUpdateUnzipper.unzip(zippedCsvFile, deleteZip);
    }

    

}
