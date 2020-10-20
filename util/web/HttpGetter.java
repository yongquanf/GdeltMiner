package util.web;

import org.apache.http.client.methods.HttpGet;

import java.net.URISyntaxException;

/**
 * @author Kevin Chen
 */
public class HttpGetter
{
    public static HttpGet get(String url)
    {
        try
        {
            validateUrl(url);
        }
        catch (URISyntaxException e)
        {
            throw new GdeltException("Invalid url: " + url, e);
        }
        return new HttpGet(url);
    }

    private static void validateUrl(String url) throws URISyntaxException
    {
        // TODO bring back
//		final String[] schemes =
//			{
//				"http", "https"
//			}; // DEFAULT schemes = "http", "https", "ftp"
//		final UrlValidator urlValidator = new UrlValidator( schemes );
//
//		if ( url == null || ( !url.contains( "//localhost" ) && !urlValidator.isValid( url ) ) )
//		{
//			throw new URISyntaxException( "URL: " + url, "URL is not valid" );
//		}
    }
}
