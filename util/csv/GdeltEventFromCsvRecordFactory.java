package util.csv;

import util.models.GdeltEventResource;
import util.web.GdeltException;

import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

/**
 * @author Kevin Chen
 */
public class GdeltEventFromCsvRecordFactory
{
    private static final Logger logger = LoggerFactory.getLogger(GdeltEventFromCsvRecordFactory.class);

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);

    public static GdeltEventResource create(CSVRecord record, Map<String, Integer> values)
    {
        GdeltEventResource gdeltEvent = null;

        gdeltEvent = new GdeltEventResource();

        String actionGeoCountryCode = record.get(values.get("ActionGeo_CountryCode"));
        gdeltEvent.setActionGeoCountryCode(actionGeoCountryCode);

        String actor1GeoCountryCode = record.get(values.get("Actor1Geo_CountryCode"));
        gdeltEvent.setActor1GeoCountryCode(actor1GeoCountryCode);

        String actor2GeoCountryCode = record.get(values.get("Actor2Geo_CountryCode"));
        gdeltEvent.setActor2GeoCountryCode(actor2GeoCountryCode);

        logger.trace("Parsing fields...");
        gdeltEvent.setGlobalEventID(Integer.parseInt(record.get(values.get("GLOBALEVENTID"))));

        String sqldate = record.get(values.get("SQLDATE"));

        try
        {
            Date eventDate = dateFormat.parse(sqldate);
            gdeltEvent.setEventDate(eventDate);
        }
        catch (ParseException e)
        {
            throw new GdeltException("Could not parse date: " + sqldate, e);
        }

        gdeltEvent.setActor1Code(record.get(values.get("Actor1Code")));

        gdeltEvent.setActor1Name(record.get(values.get("Actor1Name")));

        gdeltEvent.setActor1CountryCode(record.get(values.get("Actor1CountryCode")));

        gdeltEvent.setActor1KnownGroupCode(record.get(values.get("Actor1KnownGroupCode")));

        gdeltEvent.setActor1EthnicCode(record.get(values.get("Actor1EthnicCode")));

        gdeltEvent.setActor1Religion1Code(record.get(values.get("Actor1Religion1Code")));

        gdeltEvent.setActor1Religion2Code(record.get(values.get("Actor1Religion2Code")));

        gdeltEvent.setActor1Type1Code(record.get(values.get("Actor1Type1Code")));

        gdeltEvent.setActor1Type2Code(record.get(values.get("Actor1Type2Code")));

        gdeltEvent.setActor1Type3Code(record.get(values.get("Actor1Type3Code")));

        gdeltEvent.setActor2Code(record.get(values.get("Actor2Code")));

        gdeltEvent.setActor2Name(record.get(values.get("Actor2Name")));

        gdeltEvent.setActor2CountryCode(record.get(values.get("Actor2CountryCode")));

        gdeltEvent.setActor2KnownGroupCode(record.get(values.get("Actor2KnownGroupCode")));

        gdeltEvent.setActor2EthnicCode(record.get(values.get("Actor2EthnicCode")));

        gdeltEvent.setActor2Religion1Code(record.get(values.get("Actor2Religion1Code")));

        gdeltEvent.setActor2Religion2Code(record.get(values.get("Actor2Religion2Code")));

        gdeltEvent.setActor2Type1Code(record.get(values.get("Actor2Type1Code")));

        gdeltEvent.setActor2Type2Code(record.get(values.get("Actor2Type2Code")));

        gdeltEvent.setActor2Type3Code(record.get(values.get("Actor2Type3Code")));

        gdeltEvent.setRootEvent(Boolean.parseBoolean(record.get(values.get("IsRootEvent"))));

        gdeltEvent.setEventCode(record.get(values.get("EventCode")));

        gdeltEvent.setEventBaseCode(record.get(values.get("EventBaseCode")));

        gdeltEvent.setEventRootCode(record.get(values.get("EventRootCode")));

        if (record.get(values.get("QuadClass")).length() > 0)
        {
            gdeltEvent.setQuadClass(Integer.parseInt(record.get(values.get("QuadClass"))));
        }

        if (record.get(values.get("GoldsteinScale")).length() > 0)
        {
            gdeltEvent.setGoldsteinScale(
                Double.parseDouble(record.get(values.get("GoldsteinScale"))));
        }

        if (record.get(values.get("NumMentions")).length() > 0)
        {
            gdeltEvent.setNumMentions(Integer.parseInt(record.get(values.get("NumMentions"))));
        }

        if (record.get(values.get("NumSources")).length() > 0)
        {
            gdeltEvent.setNumSources(Integer.parseInt(record.get(values.get("NumSources"))));
        }

        if (record.get(values.get("NumArticles")).length() > 0)
        {
            gdeltEvent.setNumArticles(Integer.parseInt(record.get(values.get("NumArticles"))));
        }

        if (record.get(values.get("AvgTone")).length() > 0)
        {
            gdeltEvent.setAvgTone(Double.parseDouble(record.get(values.get("AvgTone"))));
        }

        if (record.get(values.get("Actor1Geo_Type")).length() > 0)
        {
            gdeltEvent.setActor1GeoType(Integer.parseInt(record.get(values.get("Actor1Geo_Type"))));
        }

        gdeltEvent.setActor1GeoFullName(record.get(values.get("Actor1Geo_FullName")));

        gdeltEvent.setActor1GeoADM1Code(record.get(values.get("Actor1Geo_ADM1Code")));

        if (values.get("Actor1Geo_ADM2Code") != null)
        {
            gdeltEvent.setActor1GeoADM2Code(record.get(values.get("Actor1Geo_ADM2Code")));
        }

        Double latitude, longitude;

        if (record.get(values.get("Actor1Geo_Lat")).length() > 0
            && record.get(values.get("Actor1Geo_Long")).length() > 0)
        {
            latitude = Double.parseDouble(record.get(values.get("Actor1Geo_Lat")));

            longitude = Double.parseDouble(record.get(values.get("Actor1Geo_Long")));

            gdeltEvent.setActor1GeoLat(latitude);
            gdeltEvent.setActor1GeoLong(longitude);
        }

        gdeltEvent.setActor1GeoFeatureID(record.get(values.get("Actor1Geo_FeatureID")));

        if (record.get(values.get("Actor2Geo_Type")).length() > 0)
        {
            gdeltEvent.setActor2GeoType(Integer.parseInt(record.get(values.get("Actor2Geo_Type"))));
        }

        gdeltEvent.setActor2GeoFullName(record.get(values.get("Actor2Geo_FullName")));

        gdeltEvent.setActor2GeoADM1Code(record.get(values.get("Actor2Geo_ADM1Code")));

        if (values.get("Actor2Geo_ADM2Code") != null)
        {
            gdeltEvent.setActor2GeoADM2Code(record.get(values.get("Actor2Geo_ADM2Code")));
        }

        if (record.get(values.get("Actor2Geo_Lat")).length() > 0
            && record.get(values.get("Actor2Geo_Long")).length() > 0)
        {
            latitude = Double.parseDouble(record.get(values.get("Actor2Geo_Lat")));

            longitude = Double.parseDouble(record.get(values.get("Actor2Geo_Long")));

            gdeltEvent.setActor2GeoLat(latitude);
            gdeltEvent.setActor2GeoLong(longitude);
        }

        gdeltEvent.setActor2GeoFeatureID(record.get(values.get("Actor2Geo_FeatureID")));

        gdeltEvent.setActionGeoType(Integer.parseInt(record.get(values.get("ActionGeo_Type"))));

        gdeltEvent.setActionGeoFullName(record.get(values.get("ActionGeo_FullName")));

        gdeltEvent.setActionGeoADM1Code(record.get(values.get("ActionGeo_ADM1Code")));

        if (values.get("ActionGeo_ADM2Code") != null)
        {
            gdeltEvent.setActionGeoADM2Code(record.get(values.get("ActionGeo_ADM2Code")));
        }

        if (record.get(values.get("ActionGeo_Lat")).length() > 0
            && record.get(values.get("ActionGeo_Long")).length() > 0)
        {
            latitude = Double.parseDouble(record.get(values.get("ActionGeo_Lat")));

            longitude = Double.parseDouble(record.get(values.get("ActionGeo_Long")));

            gdeltEvent.setActionGeoLat(latitude);
            gdeltEvent.setActionGeoLong(longitude);
        }

        gdeltEvent.setActionGeoFeatureID(record.get(values.get("ActionGeo_FeatureID")));

        gdeltEvent.setDateAdded(record.get(values.get("DATEADDED")));

        return gdeltEvent;
    }
}
