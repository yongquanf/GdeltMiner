package util.web;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * TODO move this to a utils repo
 *
 * @author Kevin Chen
 */
public class FileUnzipper
{
    private static final Logger logger = LoggerFactory.getLogger(FileUnzipper.class);

    public static Boolean unzipFile(File outputDirectory, String zipFile)
    {
        String destinationFolder = outputDirectory.getAbsolutePath();

        // if the output directory doesn't exist, create it
        if (!outputDirectory.exists())
        {
            outputDirectory.mkdirs();
        }

        try (
            FileInputStream fInput = new FileInputStream(zipFile);
            ZipInputStream zipInput = new ZipInputStream(fInput))
        {
            ZipEntry entry = zipInput.getNextEntry();

            while (entry != null)
            {
                String entryName = entry.getName();
                File file = new File(destinationFolder + File.separator + entryName);

                logger.debug("Unzip file \"{}\" to \"{}\"", entryName, file.getAbsolutePath());

                // create the directories of the zip directory
                if (entry.isDirectory())
                {
                    File newDir = new File(file.getAbsolutePath());
                    if (!newDir.exists())
                    {
                        boolean success = newDir.mkdirs();
                        if (!success)
                        {
                            logger.error("Zipping file failed ");
                            return false;
                        }
                    }
                }
                else
                {
                    try (FileOutputStream fOutput = new FileOutputStream(file))
                    {
                        IOUtils.copy(zipInput, fOutput);
                    }
                    catch (FileNotFoundException ex)
                    {
                        logger.error("File not found", ex);
                    }
                }

                // close ZipEntry and take the next one
                zipInput.closeEntry();
                entry = zipInput.getNextEntry();
            }

            // close the last ZipEntry
            zipInput.closeEntry();

            return true;
        }
        catch (IOException e)
        {
            return false;
        }
    }
}
