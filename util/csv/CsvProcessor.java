package util.csv;

import util.models.GdeltDownloadResultResource;
import util.models.GdeltEventResource;
import util.web.GdeltException;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Processes a GDELT CSV file.
 *
 * @author Kevin Chen
 */
public class CsvProcessor
{
    private static final Logger logger = LoggerFactory.getLogger(CsvProcessor.class);

    private CSVParser createCvsParser(Reader reader, CSVFormat csvFileFormat)
    {
        try
        {
            return new CSVParser(reader, csvFileFormat);
        }
        catch (IOException e)
        {
            throw new GdeltException("Could not construct CSV parser", e);
        }
    }

    /**
     * process
     * @param file
     * @return
     */
    public GDELTReturnResult processCSV(File file)
    {
        // GDELT CSV files are tab delimited
        CSVFormat csvFormat = CSVFormat.newFormat('\t');

        logger.debug("Creating csv parser...");
        Reader reader = null;
        try
        {
            reader = new FileReader(file);
        }
        catch (FileNotFoundException e)
        {
            throw new GdeltException("File not found", e);
        }
        CSVParser csvParser = createCvsParser(reader, csvFormat);

        return processCSV(csvParser, csvFormat);
    }

    public GDELTReturnResult processCSV(InputStream inputStream)
    {
        // GDELT CSV files are tab delimited
        CSVFormat csvFormat = CSVFormat.newFormat('\t');

        logger.debug("Creating csv parser...");
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        Reader reader = IOUtils.toBufferedReader(inputStreamReader);
        CSVParser csvParser = createCvsParser(reader, csvFormat);

        return processCSV(csvParser, csvFormat);
    }

    /**
     * process cvs
     * @param csvParser
     * @param csvFormat
     * @return
     */
    private GDELTReturnResult processCSV(CSVParser csvParser, CSVFormat csvFormat)
    {
        List<GdeltEventResource> gdeltEventList = new ArrayList<>();

        GDELTReturnResult gdeltResult = new GDELTReturnResult();

        GdeltDownloadResultResource gdeltDownloadResultResource = new GdeltDownloadResultResource();
        gdeltResult.setGdeltEventList(new ArrayList<>());

        gdeltResult.setDownloadResult(gdeltDownloadResultResource);

        int recordsLoaded = 0;
        int recordsFailed = 0;

        logger.debug("Retrieving csv records...");
        List<CSVRecord> csvRecords = null;
        try
        {
            csvRecords = csvParser.getRecords();
        }
        catch (IOException e)
        {
            throw new GdeltException("Could not get records from csv", e);
        }

        if (csvRecords.isEmpty())
        {
            logger.debug("Found 0 records");
        }
        else
        {
            String[] values = null;

            CSVRecord headerRecord = csvRecords.get(0);

            int size = headerRecord.size();
            logger.debug("Found {} headers", size);

            // TODO what's the point of having multiple header enums if we're hard-coding the headers below???
            if (size == GdeltOldColumnHeader.getSize())
            {
                logger.debug("using GdeltOldColumnHeader headers");
                values = GdeltOldColumnHeader.getNames();
            }
            else if (size == Gdelt1_0ColumnHeader.getSize())
            {
                logger.debug("using Gdelt1_0ColumnHeader headers");
                values = Gdelt1_0ColumnHeader.getNames();
            }
            else if (size == Gdelt2_0ColumnHeader.getSize())
            {
                logger.debug("using Gdelt2_0ColumnHeader headers");
                values = Gdelt2_0ColumnHeader.getNames();
            }
            else
            {
                List<String> headers = new ArrayList<>();
                headerRecord.iterator().forEachRemaining(headers::add);
                String concatenatedHeaders = StringUtils.join(headers, "|");
                throw new GdeltException(String.format("Unexpected number of headers (%d): %s", size, concatenatedHeaders));
            }

            if (values != null)
            {
                csvFormat.withHeader(values);

                Map<String, Integer> map = new HashMap<>();
                for (int i = 0; i < values.length; i++)
                {
                    map.put(values[i], i);
                }

                logger.debug("Process GDELT events...");
                GDELTReturnResult result = processEvents(csvRecords, map);
                logger.debug("Finished processing GDELT events");

                // add to the main result list
                gdeltEventList.addAll(result.getGdeltEventList());

                // add to the main failed and loaded record count
                GdeltDownloadResultResource d = result.getDownloadResult();

                recordsFailed += d.getRecordsFailed();
                recordsLoaded += d.getRecordsLoaded();
            }
        }

        gdeltDownloadResultResource.setDownloadedSuccessfully(true);
        gdeltDownloadResultResource.setRecordsFailed(recordsFailed);
        gdeltDownloadResultResource.setRecordsLoaded(recordsLoaded);

        gdeltResult.setDownloadResult(gdeltDownloadResultResource);

        gdeltResult.setGdeltEventList(gdeltEventList);
        return gdeltResult;
    }

    private GDELTReturnResult processEvents(List<CSVRecord> records, Map<String, Integer> values)
    {
        GDELTReturnResult gdeltResult = new GDELTReturnResult();
        List<GdeltEventResource> gdeltEventList = new ArrayList<>();
        int recordsLoaded = 0;
        int recordsFailed = 0;

        for (int i = 0; i < records.size(); i++)
        {
            logger.trace("Parsing record {}", i);
            CSVRecord record = records.get(i);

            GdeltEventResource gdeltEvent;
            try
            {
                gdeltEvent = GdeltEventFromCsvRecordFactory.create(record, values);
            }
            catch (Exception e)
            {
                recordsFailed++;
                continue;
            }

            gdeltEvent.setSourceUrl(record.get(values.get("SOURCEURL")));

            recordsLoaded = recordsLoaded + 1;
            gdeltEventList.add(gdeltEvent);
        }

        logger.debug("Went through {} records", records.size());

        GdeltDownloadResultResource gdeltDownload = new GdeltDownloadResultResource();

        gdeltDownload.setDownloadedSuccessfully(true);
        gdeltDownload.setRecordsFailed(recordsFailed);
        gdeltDownload.setRecordsLoaded(recordsLoaded);

        logger.debug(gdeltDownload.toString());

        gdeltResult.setDownloadResult(gdeltDownload);

        gdeltResult.setGdeltEventList(gdeltEventList);

        return gdeltResult;
    }
}
