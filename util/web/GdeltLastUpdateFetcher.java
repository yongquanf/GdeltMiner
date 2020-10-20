package util.web;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Retrieves the text at {@code http://data.gdeltproject.org/gdelt_v2/lastupdate.txt}.
 * <p>
 * <pre>
 *     {@code
 * 175564 7bf8faa47ede943fa1ba4b78950915cd http://data.gdeltproject.org/gdeltv2/20161013141500.export.CSV.zip
 * 340347 04ede6e06c3c1949c433642799e20158 http://data.gdeltproject.org/gdeltv2/20161013141500.mentions.CSV.zip
 * 16452276 5adbe936332ff09d29ae62430eb8eb3f http://data.gdeltproject.org/gdeltv2/20161013141500.gkg.csv.zip
 *     }
 * </pre>
 *
 * @author Kevin Chen
 */
public class GdeltLastUpdateFetcher
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltLastUpdateFetcher.class);

    public String getGDELTLastUpdate(HttpClient httpClient, GdeltConfiguration gdeltConfiguration)
    {
        String gdeltV2ServerURL = gdeltConfiguration.getGdeltLastUpdateURL();

        String csvLocation = null;

        HttpGet httpget = HttpGetter.get(gdeltV2ServerURL);
        HttpResponse response;
        long start = System.currentTimeMillis();
        long end;
        try
        {
            response = httpClient.execute(httpget);
        }
        catch (Exception e)
        {
            end = System.currentTimeMillis();
            logger.error("Failed to reach {} after {} ms. Socket timeout: {}, Connection timeout: {}",
                gdeltV2ServerURL, end - start, HttpClientFactory.SOCKET_TIMEOUT, HttpClientFactory.CONNECTION_TIMEOUT);
            logger.error("GDELT failed", e);
            throw new GdeltException("Could not execute request for GDELT last update", e);
        }

        end = System.currentTimeMillis();
        logger.debug("Successfully reached {} after {} ms. Socket timeout: {}, Connection timeout: {}",
            gdeltV2ServerURL, end - start, HttpClientFactory.SOCKET_TIMEOUT, HttpClientFactory.CONNECTION_TIMEOUT);

        if (response.getStatusLine().getStatusCode() != 200)
        {
            throw new GdeltException("Response not OK");
        }

        HttpEntity httpEntity = response.getEntity();
        String update = null;
        try
        {
            update = EntityUtils.toString(httpEntity);
        }
        catch (IOException e)
        {
            throw new GdeltException("Could not read http entity from GDELT last update response", e);
        }

        logger.debug("FOUND RESPONSE: {}", update);

        return parseLastUpdateLocation(update);
    }

    String parseLastUpdateLocation(String update)
    {
        String csvLocation;

        String[] updateEntries = StringUtils.split(update, ' ');
        if (updateEntries == null || updateEntries.length <= 3)
        {
            throw new GdeltException("GDELT last update text is supposed to have at least 3 entries. Instead we got: " + update);
        }
        else
        {
            csvLocation = updateEntries[2];

            int carriageReturn = csvLocation.indexOf("\n");

            if (carriageReturn > 0)
            {
                csvLocation = csvLocation.split("\n")[0];
            }
        }

        logger.debug("CSV LOCATION: {}", csvLocation);

        return csvLocation;
    }
}
