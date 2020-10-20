package util.web;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * @author yinlei
 * @since 2018/10/23 9:55
 */
public class GDELTUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GDELTUtils.class);

    /**
     * 解压zip文件，返回流
     *
     * @param originFile zip文件。
     * @return 如果目标文件不是zip文件抛异常
     */
    public static InputStream decompress(File originFile) {
        InputStream is = null;
        try {
            ZipFile zipFile = new ZipFile(originFile);
            ZipEntry zipEntry;
            Enumeration<? extends ZipEntry> entry = zipFile.entries();
            while (entry.hasMoreElements()) { // 这个处理多个，其实我们的业务就一个
                zipEntry = entry.nextElement();
                is = zipFile.getInputStream(zipEntry);
            }
        } catch (Exception e) {
            LOGGER.error("解压zip文件错误。" + e.getMessage());
        }
        return is;
    }

    /**
     * 解压zip格式的数据
     * @param data 待解压的数据，ISO8859-1格式
     * @return 解压后的数据，格式由压缩前的数据格式决定
     */
    public static byte[] unzip(byte[] data) {
        if (data == null) {
            return null;
        }
        ByteArrayOutputStream out = null;
        ByteArrayInputStream in = null;
        ZipInputStream zin = null;
        byte[] result = null;
        try {
            out = new ByteArrayOutputStream();
            in = new ByteArrayInputStream(data);
            zin = new ZipInputStream(in);
            zin.getNextEntry();
            byte[] buffer = new byte[1024];
            int offset;
            while ((offset = zin.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            result = out.toByteArray();
        } catch (IOException e) {
            LOGGER.error("zip解压字符IO错误，msg=[{}]。", e.getMessage());
        } finally {
            IOUtils.closeQuietly(zin);
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
        return result;
    }

    public static byte[] get(String url) {
        try {
            URL httpUrl = new URL(url);
            HttpURLConnection connection = getHttpConnection(httpUrl);
            connection.connect();
            return getBytes(connection);
        } catch (MalformedURLException e) {
            LOGGER.error("url format error, msg=[{}].", e.getMessage());
        } catch (IOException e) {
            LOGGER.error("http connection io error., msg=[{}].", e);
        }
        return null;
    }

    private static HttpURLConnection getHttpConnection(URL url) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setConnectTimeout(24 * 1000);
            connection.setReadTimeout(50 * 1000);
            connection.setRequestMethod("GET");
        } catch (Exception e) {
            LOGGER.error("build http url connection error, msg=[{}].", e.getMessage());
        }
        return connection;
    }

    private static byte[] getBytes(HttpURLConnection connection) throws IOException {
        int code = connection.getResponseCode();
        if (code == 200) {
            InputStream inputStream = connection.getInputStream();
            return IOUtils.toByteArray(inputStream);
        } else {
            LOGGER.error("invoke error, code=[{}]", code);
        }
        return null;
    }

    /**
     * 返回 [经度，纬度]
     * @param record csv record
     * @return [经度，纬度]
     */
    public static Double[] getLatAndLng(CSVRecord record) {
        String lat = record.get(56);
        if (StringUtils.isBlank(lat)) {
            lat = record.get(48);
            if (StringUtils.isBlank(lat)) {
                lat = record.get(40);
            }
        }
        String lng = record.get(57);
        if (StringUtils.isBlank(lng)) {
            lng = record.get(49);
            if (StringUtils.isBlank(lng)) {
                lng = record.get(41);
            }
        }

        double latitude;
        double longitude;
        if (StringUtils.isAnyBlank(lat, lng)) {
            latitude = getLat();
            longitude = getLng();
//            LOGGER.debug("没有获取到经纬度,模拟的经纬度是lat=[" + latitude + "], lng=[" + longitude + "]");
        } else {
//            LOGGER.debug("经纬度是lat=[" + lat + "], lng=[" + lng + "]");
            latitude = Double.parseDouble(lat);
            longitude = Double.parseDouble(lng);
        }
        return new Double[]{longitude, latitude};
    }

    public static double getLatOrLng(double min, double max) {
        BigDecimal db = new BigDecimal(Math.random() * (max - min) + min);
        return db.setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue(); // 小数后6位
    }

    /**
     * 维度的范围 [-90,90]
     *
     * @return 维度
     */
    public static double getLat() {
        return getLatOrLng(3, 53);
    }

    /**
     * 经度的范围 [-180,180]
     *
     * @return 经度
     */
    public static double getLng() {
        return getLatOrLng(73, 135);
    }
}