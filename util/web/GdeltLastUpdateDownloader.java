package util.web;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Kevin Chen
 */
public class GdeltLastUpdateDownloader
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltLastUpdateDownloader.class);

    private void ensureDirectoryExists(File directory)
    {
        // if the output directory doesn't exist, create it
        if (!directory.exists())
        {
            directory.mkdirs();
        }
    }

    /**
     * Downloads the zipped csv and saves it in a directory of your choosing, as {@code <DATE>.export.CSV.zip}
     *
     * @param httpClient
     * @param directory
     * @param lastUpdateUrl
     * @return
     */
    public File downloadGDELTZipFile(HttpClient httpClient, File directory, String lastUpdateUrl)
    {
        ensureDirectoryExists(directory);

        if (lastUpdateUrl == null)
        {
            throw new IllegalArgumentException("lastUpdateUrl cannot be null");
        }

        String fileDestination = directory.getAbsolutePath();

        logger.debug("Downloading zipped CSV file to {}", fileDestination);

        if (!UrlValidator.isValid(lastUpdateUrl))
        {
            logger.error("GDELT url is invalid: {}", lastUpdateUrl);
            return null;
        }

        logger.debug("Download zipped CSV file from: {}", lastUpdateUrl);

        // e.g,. 20161014131500.export.CSV.zip
        String zipFilename = lastUpdateUrl.substring(lastUpdateUrl.lastIndexOf("/") + 1);

        logger.debug("Retrieving zip file: {}", zipFilename);

        File zipFile = new File(fileDestination + File.separator + zipFilename);

        boolean downloadStatus = downloadFile(httpClient, lastUpdateUrl, zipFile);

        return zipFile;
    }

    /**
     * Returns the file the zipped csv was download to.
     *
     * @param httpClient
     * @param lastUpdateUrl
     * @param zipFile
     * @return
     */
    boolean downloadFile(HttpClient httpClient, String lastUpdateUrl, File zipFile)
    {
        HttpGet httpGet = HttpGetter.get(lastUpdateUrl);
        HttpResponse response = null;
        try
        {
            response = httpClient.execute(httpGet);
        }
        catch (IOException e)
        {
            throw new GdeltException("Could not execute request", e);
        }

        if (response.getStatusLine().getStatusCode() == 200)
        {
            try (FileOutputStream fos = new FileOutputStream(zipFile))
            {
                response.getEntity().writeTo(fos);
                return true;
            }
            catch (IOException e)
            {
                throw new GdeltException("Could not get response", e);
            }
        }
        return false;
    }
}
