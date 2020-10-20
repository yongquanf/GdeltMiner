package util.web;

import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * This client will use the newly created GDELT gateway to get data.
 *
 * @author Kevin Chen
 */
class HttpClientFactory
{
    private static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

    public static final int CONNECTION_TIMEOUT = 120000;

    public static final int SOCKET_TIMEOUT = 120000;

    public static DefaultHttpClient createHttpClient(HttpHost proxy)
    {
        HttpParams httpParams = new BasicHttpParams();
        // Timeout in n/1000 seconds 1 minute
        HttpConnectionParams.setConnectionTimeout(httpParams, CONNECTION_TIMEOUT);
        // Timeout in n/1000 seconds
        HttpConnectionParams.setSoTimeout(httpParams, SOCKET_TIMEOUT);

        if (proxy != null)
        {
            httpParams.setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }

        DefaultHttpClient hc = new DefaultHttpClient(httpParams);
        configureCookieStore(hc);
        configureSSLHandling(hc);
        return hc;
    }

    private static void configureCookieStore(DefaultHttpClient hc)
    {
        CookieStore cStore = new BasicCookieStore();
        hc.setCookieStore(cStore);
    }

    private static void configureSSLHandling(DefaultHttpClient hc)
    {
        SSLSocketFactory sf = buildSSLSocketFactory();
        Scheme https = new Scheme("https", 443, sf);
        SchemeRegistry sr = hc.getConnectionManager().getSchemeRegistry();
        sr.register(https);
    }

    private static SSLSocketFactory buildSSLSocketFactory()
    {
        TrustStrategy ts = new TrustStrategy()
        {
            @Override
            public boolean isTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException
            {
                // heck yea!
                return true;
            }
        };

        SSLSocketFactory sf = null;
        try
        {
            /* build socket factory with hostname verification turned off. */
            sf = new SSLSocketFactory(ts, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        }
        catch (KeyManagementException | KeyStoreException | UnrecoverableKeyException | NoSuchAlgorithmException ex)
        {
            logger.error("Failed to create an SSL Factory connection", ex);
        }

        return sf;
    }
}
