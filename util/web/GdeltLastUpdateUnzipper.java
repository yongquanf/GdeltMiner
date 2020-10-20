package util.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * @author Kevin Chen
 */
public class GdeltLastUpdateUnzipper
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltLastUpdateUnzipper.class);

    public File unzip(File zipFile, boolean deleteZipFileAfterDownload)
    {
        if (zipFile == null)
        {
            throw new IllegalArgumentException("Cannot unzip a null file");
        }

        File zipFileDirectory = zipFile.getParentFile();
        String zipFileAbsolutePath = zipFile.getAbsolutePath();

        try
        {
            Boolean unzipStatus;

            // TODO constructor DI instead of static method
            unzipStatus = FileUnzipper.unzipFile(zipFileDirectory, zipFileAbsolutePath);

            if (unzipStatus)
            {
                String zipFilename = zipFile.getName();
                String unzipFilename = zipFilename.substring(0, zipFilename.lastIndexOf("."));
                return new File(zipFileDirectory.getAbsolutePath() + File.separator + unzipFilename);
            }
        }
        catch (Exception ex)
        {
            logger.error("Exception while unzipping file: \"{}\"", zipFileAbsolutePath, ex);
        }
        finally
        {
            File fileTemp = new File(zipFileAbsolutePath);
            if (deleteZipFileAfterDownload && fileTemp.exists())
            {
                fileTemp.delete();
            }
        }

        return null;
    }
}
