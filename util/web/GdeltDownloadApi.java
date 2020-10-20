package util.web;

import util.csv.GDELTReturnResult;
import util.csv.GdeltCSVParser;
import util.models.GdeltEventResource;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Kevin Chen
 */
public class GdeltDownloadApi
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltDownloadApi.class);

    private static File getDefaultTestDirectory()
    {
        return new File(System.getProperty("user.home") + File.separator + "gdelt-test");
    }

    enum GdeltCameoDownloadCodes
    {
        EngageInMaterialCooperation("06"),
        ProvideAid("07"),
        Threaten("13"),
        Protest("14"),
        Coerce("17"),
        Assault("18"),
        Fight("19"),
        EngageInUnconventionalMassViolence("20");

        public String getRootCameoCode()
        {
            return rootCameoCode;
        }

        GdeltCameoDownloadCodes(String cameoCode)
        {
            this.rootCameoCode = cameoCode;
        }

        public static boolean containsCameo(String eventRootCode)
        {
            return eventRootCode.equals(EngageInMaterialCooperation.getRootCameoCode()) ||
                eventRootCode.equals(ProvideAid.getRootCameoCode()) ||
                eventRootCode.equals(Threaten.getRootCameoCode()) ||
                eventRootCode.equals(Protest.getRootCameoCode()) ||
                eventRootCode.equals(Coerce.getRootCameoCode()) ||
                eventRootCode.equals(Assault.getRootCameoCode()) ||
                eventRootCode.equals(Fight.getRootCameoCode()) ||
                eventRootCode.equals(EngageInUnconventionalMassViolence.getRootCameoCode());
        }

        private final String rootCameoCode;
    }

    private GdeltWebFetchApi gdeltApi;
    private GdeltCSVParser cvsparser;

    @BeforeClass
    public static void initClass() throws IOException
    {
        File dir = getDefaultTestDirectory();
        dir.mkdirs();
        FileUtils.cleanDirectory(dir);
    }

    @Before
    public void init()
    {
        gdeltApi = new GdeltWebFetchApi();
        cvsparser = new GdeltCSVParser();
    }

    @Test
    public void shouldSkipDownloadIfFileFound()
    {
        gdeltApi.downloadUpdate(getDefaultTestDirectory(), true, false, 2016, 8, 5, 13, 30);
        gdeltApi.downloadUpdate(getDefaultTestDirectory(), true, false, 2016, 8, 6, 13, 45);
    }

    @Test
    public void testSpecificDownload()
    {
        File file0 = gdeltApi.downloadUpdate(new File("src/test/resources"), true, false, 2016, 8, 5, 13, 15);
        File file1 = gdeltApi.download().toDirectory(getDefaultTestDirectory()).atTime(OffsetDateTime.of(2016, 8, 5, 13, 30, 0, 0, ZoneOffset.UTC)).execute();
        File file2 = gdeltApi.download().toDirectory(getDefaultTestDirectory()).atTime(LocalDateTime.now().minusHours(1).withMinute(0)).execute();
        System.out.println(file2.length());
    }

    @Test
    public void testDownloadSince()
    {
        gdeltApi.downloadAllSince(LocalDateTime.now().minusHours(1)).toDirectory(getDefaultTestDirectory()).execute();
    }

    @Test
    public void testDownloadBetween()
    {
        LocalDateTime since = LocalDateTime.now().minusHours(3);
        LocalDateTime until = LocalDateTime.now().minusHours(2);
        gdeltApi.downloadAllBetween(since, until).toDirectory(getDefaultTestDirectory()).execute();
    }

    @Test
    public void testDownload()
    {
        // Download LastUpdate CSV
        File csvFile = gdeltApi.downloadLastUpdate().toDirectory(getDefaultTestDirectory()).execute();
        assertNotNull(csvFile);
        logger.info("Download a GDELT CSV file to: {}", csvFile.getAbsolutePath());
    }

    @Test
    public void testParseCsvInputStream() throws IOException
    {
        // TODO csv processing works with file but not with InputStream
//        InputStream inputStream = GdeltApiTest.class.getClassLoader().getResourceAsStream("20161013151500.export.CSV");
//        logger.info("Input Stream: {}", IOUtils.toString(inputStream, StandardCharsets.UTF_8));
//        GDELTReturnResult gdeltReturnResult = gdeltApi.parseCsv(inputStream);
//        assertCsv(gdeltReturnResult);
    }

    @Test
    public void testParseCsvFile()
    {
        File file = new File(GdeltDownloadApi.class.getClassLoader().getResource("20161013151500.export.CSV").getFile());
        GDELTReturnResult gdeltReturnResult = cvsparser.parseCsv(file);
        assertCsv(gdeltReturnResult);

        List<GdeltEventResource> gdeltEvents = gdeltReturnResult.getGdeltEventList();
        GdeltEventResource first = gdeltEvents.get(0);
        assertEquals(588604779, first.getGlobalEventID());
        assertEquals("", first.getActor1Code());
        assertEquals("DEU", first.getActor2Code());
        assertEquals("HALLE", first.getActor2Name());
        assertEquals("DEU", first.getActor2CountryCode());
        assertEquals(false, first.getRootEvent());
        assertEquals("040", first.getEventCode());
        assertEquals("040", first.getEventBaseCode());
        assertEquals("04", first.getEventRootCode());
        assertEquals(1, first.getQuadClass());
    }

    private void assertCsv(GDELTReturnResult gdeltReturnResult)
    {
        long start, end;
        gdeltReturnResult.getGdeltEventList().forEach(e -> logger.trace(e.toString()));

        List<GdeltEventResource> gdeltEvents = gdeltReturnResult.getGdeltEventList();
        start = System.currentTimeMillis();
        long count = gdeltEvents.stream().filter(event -> GdeltCameoDownloadCodes.containsCameo(event.getEventRootCode())).count();
        end = System.currentTimeMillis();
        logger.debug("Took {} ms to filter", end - start);
        logger.debug("Loaded {} events", count);
        assertEquals(653, count);
    }
}
