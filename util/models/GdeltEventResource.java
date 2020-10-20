package util.models;

import java.io.Serializable;
import java.util.Date;

/**
 * Country codes are stored as FIPS 10-4 strings.
 *
 * @author Kevin Chen
 */
public class GdeltEventResource implements Serializable
{
	


	private int globalEventID;

    private String actor1Code;

    private String actor1Name;

    private String actor1CountryCode;

    private String actor1KnownGroupCode;

    private String actor1EthnicCode;

    private String actor1Religion1Code;

    private String actor1Religion2Code;

    private String actor1Type1Code;

    private String actor1Type2Code;

    private String actor1Type3Code;

    private String actor2Code;

    private String actor2Name;

    private String actor2CountryCode;

    private String actor2KnownGroupCode;

    private String actor2EthnicCode;

    private String actor2Religion1Code;

    private String actor2Religion2Code;

    private String actor2Type1Code;

    private String actor2Type2Code;

    private String actor2Type3Code;

    private Boolean isRootEvent;

    private String eventCode;

    private String eventBaseCode;

    private String eventRootCode;

    private int quadClass;

    private Double goldsteinScale;

    private int numMentions;

    private int numSources;

    private int numArticles;

    private Double avgTone;

    private int actor1GeoType;

    private String actor1GeoFullName;

    private String actor1GeoCountryCode;

    private String actor1GeoADM1Code;

    private String actor1GeoADM2Code;

    private double actor1GeoLat;

    private double actor1GeoLong;

    private String actor1GeoFeatureID;

    private int actor2GeoType;

    private String actor2GeoFullName;

    private String actor2GeoCountryCode;

    private String actor2GeoADM1Code;

    private String actor2GeoADM2Code;

    private double actor2GeoLat;

    private double actor2GeoLong;

    private String actor2GeoFeatureID;

    private int actionGeoType;

    private String actionGeoFullName;

    private String actionGeoCountryCode;

    private String actionGeoADM1Code;

    private String actionGeoADM2Code;

    private double actionGeoLat;

    private double actionGeoLong;

    private String actionGeoFeatureID;

    private String dateAdded;

    private String sourceUrl;

    private Date eventDate;

    public int getGlobalEventID()
    {
        return globalEventID;
    }

    public void setGlobalEventID(int globalEventID)
    {
        this.globalEventID = globalEventID;
    }

    public String getActor1Code()
    {
        return actor1Code;
    }

    public void setActor1Code(String actor1Code)
    {
        this.actor1Code = actor1Code;
    }

    public String getActor1Name()
    {
        return actor1Name;
    }

    public void setActor1Name(String actor1Name)
    {
        this.actor1Name = actor1Name;
    }

    public String getActor1CountryCode()
    {
        return actor1CountryCode;
    }

    public void setActor1CountryCode(String actor1CountryCode)
    {
        this.actor1CountryCode = actor1CountryCode;
    }

    public String getActor1KnownGroupCode()
    {
        return actor1KnownGroupCode;
    }

    public void setActor1KnownGroupCode(String actor1KnownGroupCode)
    {
        this.actor1KnownGroupCode = actor1KnownGroupCode;
    }

    public String getActor1EthnicCode()
    {
        return actor1EthnicCode;
    }

    public void setActor1EthnicCode(String actor1EthnicCode)
    {
        this.actor1EthnicCode = actor1EthnicCode;
    }

    public String getActor1Religion1Code()
    {
        return actor1Religion1Code;
    }

    public void setActor1Religion1Code(String actor1Religion1Code)
    {
        this.actor1Religion1Code = actor1Religion1Code;
    }

    public String getActor1Religion2Code()
    {
        return actor1Religion2Code;
    }

    public void setActor1Religion2Code(String actor1Religion2Code)
    {
        this.actor1Religion2Code = actor1Religion2Code;
    }

    public String getActor1Type1Code()
    {
        return actor1Type1Code;
    }

    public void setActor1Type1Code(String actor1Type1Code)
    {
        this.actor1Type1Code = actor1Type1Code;
    }

    public String getActor1Type2Code()
    {
        return actor1Type2Code;
    }

    public void setActor1Type2Code(String actor1Type2Code)
    {
        this.actor1Type2Code = actor1Type2Code;
    }

    public String getActor1Type3Code()
    {
        return actor1Type3Code;
    }

    public void setActor1Type3Code(String actor1Type3Code)
    {
        this.actor1Type3Code = actor1Type3Code;
    }

    public String getActor2Code()
    {
        return actor2Code;
    }

    public void setActor2Code(String actor2Code)
    {
        this.actor2Code = actor2Code;
    }

    public String getActor2Name()
    {
        return actor2Name;
    }

    public void setActor2Name(String actor2Name)
    {
        this.actor2Name = actor2Name;
    }

    public String getActor2CountryCode()
    {
        return actor2CountryCode;
    }

    public void setActor2CountryCode(String actor2CountryCode)
    {
        this.actor2CountryCode = actor2CountryCode;
    }

    public String getActor2KnownGroupCode()
    {
        return actor2KnownGroupCode;
    }

    public void setActor2KnownGroupCode(String actor2KnownGroupCode)
    {
        this.actor2KnownGroupCode = actor2KnownGroupCode;
    }

    public String getActor2EthnicCode()
    {
        return actor2EthnicCode;
    }

    public void setActor2EthnicCode(String actor2EthnicCode)
    {
        this.actor2EthnicCode = actor2EthnicCode;
    }

    public String getActor2Religion1Code()
    {
        return actor2Religion1Code;
    }

    public void setActor2Religion1Code(String actor2Religion1Code)
    {
        this.actor2Religion1Code = actor2Religion1Code;
    }

    public String getActor2Religion2Code()
    {
        return actor2Religion2Code;
    }

    public void setActor2Religion2Code(String actor2Religion2Code)
    {
        this.actor2Religion2Code = actor2Religion2Code;
    }

    public String getActor2Type1Code()
    {
        return actor2Type1Code;
    }

    public void setActor2Type1Code(String actor2Type1Code)
    {
        this.actor2Type1Code = actor2Type1Code;
    }

    public String getActor2Type2Code()
    {
        return actor2Type2Code;
    }

    public void setActor2Type2Code(String actor2Type2Code)
    {
        this.actor2Type2Code = actor2Type2Code;
    }

    public String getActor2Type3Code()
    {
        return actor2Type3Code;
    }

    public void setActor2Type3Code(String actor2Type3Code)
    {
        this.actor2Type3Code = actor2Type3Code;
    }

    public Boolean getRootEvent()
    {
        return isRootEvent;
    }

    public void setRootEvent(Boolean rootEvent)
    {
        isRootEvent = rootEvent;
    }

    public String getEventCode()
    {
        return eventCode;
    }

    public void setEventCode(String eventCode)
    {
        this.eventCode = eventCode;
    }

    public String getEventBaseCode()
    {
        return eventBaseCode;
    }

    public void setEventBaseCode(String eventBaseCode)
    {
        this.eventBaseCode = eventBaseCode;
    }

    public String getEventRootCode()
    {
        return eventRootCode;
    }

    public void setEventRootCode(String eventRootCode)
    {
        this.eventRootCode = eventRootCode;
    }

    public int getQuadClass()
    {
        return quadClass;
    }

    public void setQuadClass(int quadClass)
    {
        this.quadClass = quadClass;
    }

    public Double getGoldsteinScale()
    {
        return goldsteinScale;
    }

    public void setGoldsteinScale(Double goldsteinScale)
    {
        this.goldsteinScale = goldsteinScale;
    }

    public int getNumMentions()
    {
        return numMentions;
    }

    public void setNumMentions(int numMentions)
    {
        this.numMentions = numMentions;
    }

    public int getNumSources()
    {
        return numSources;
    }

    public void setNumSources(int numSources)
    {
        this.numSources = numSources;
    }

    public int getNumArticles()
    {
        return numArticles;
    }

    public void setNumArticles(int numArticles)
    {
        this.numArticles = numArticles;
    }

    public Double getAvgTone()
    {
        return avgTone;
    }

    public void setAvgTone(Double avgTone)
    {
        this.avgTone = avgTone;
    }

    public int getActor1GeoType()
    {
        return actor1GeoType;
    }

    public void setActor1GeoType(int actor1GeoType)
    {
        this.actor1GeoType = actor1GeoType;
    }

    public String getActor1GeoFullName()
    {
        return actor1GeoFullName;
    }

    public void setActor1GeoFullName(String actor1GeoFullName)
    {
        this.actor1GeoFullName = actor1GeoFullName;
    }

    public String getActor1GeoCountryCode()
    {
        return actor1GeoCountryCode;
    }

    public void setActor1GeoCountryCode(String actor1GeoCountryCode)
    {
        this.actor1GeoCountryCode = actor1GeoCountryCode;
    }

    public String getActor1GeoADM1Code()
    {
        return actor1GeoADM1Code;
    }

    public void setActor1GeoADM1Code(String actor1GeoADM1Code)
    {
        this.actor1GeoADM1Code = actor1GeoADM1Code;
    }

    public String getActor1GeoADM2Code()
    {
        return actor1GeoADM2Code;
    }

    public void setActor1GeoADM2Code(String actor1GeoADM2Code)
    {
        this.actor1GeoADM2Code = actor1GeoADM2Code;
    }

    public double getActor1GeoLat()
    {
        return actor1GeoLat;
    }

    public void setActor1GeoLat(double actor1GeoLat)
    {
        this.actor1GeoLat = actor1GeoLat;
    }

    public double getActor1GeoLong()
    {
        return actor1GeoLong;
    }

    public void setActor1GeoLong(double actor1GeoLong)
    {
        this.actor1GeoLong = actor1GeoLong;
    }

    public String getActor1GeoFeatureID()
    {
        return actor1GeoFeatureID;
    }

    public void setActor1GeoFeatureID(String actor1GeoFeatureID)
    {
        this.actor1GeoFeatureID = actor1GeoFeatureID;
    }

    public int getActor2GeoType()
    {
        return actor2GeoType;
    }

    public void setActor2GeoType(int actor2GeoType)
    {
        this.actor2GeoType = actor2GeoType;
    }

    public String getActor2GeoFullName()
    {
        return actor2GeoFullName;
    }

    public void setActor2GeoFullName(String actor2GeoFullName)
    {
        this.actor2GeoFullName = actor2GeoFullName;
    }

    public String getActor2GeoCountryCode()
    {
        return actor2GeoCountryCode;
    }

    public void setActor2GeoCountryCode(String actor2GeoCountryCode)
    {
        this.actor2GeoCountryCode = actor2GeoCountryCode;
    }

    public String getActor2GeoADM1Code()
    {
        return actor2GeoADM1Code;
    }

    public void setActor2GeoADM1Code(String actor2GeoADM1Code)
    {
        this.actor2GeoADM1Code = actor2GeoADM1Code;
    }

    public String getActor2GeoADM2Code()
    {
        return actor2GeoADM2Code;
    }

    public void setActor2GeoADM2Code(String actor2GeoADM2Code)
    {
        this.actor2GeoADM2Code = actor2GeoADM2Code;
    }

    public double getActor2GeoLat()
    {
        return actor2GeoLat;
    }

    public void setActor2GeoLat(double actor2GeoLat)
    {
        this.actor2GeoLat = actor2GeoLat;
    }

    public double getActor2GeoLong()
    {
        return actor2GeoLong;
    }

    public void setActor2GeoLong(double actor2GeoLong)
    {
        this.actor2GeoLong = actor2GeoLong;
    }

    public String getActor2GeoFeatureID()
    {
        return actor2GeoFeatureID;
    }

    public void setActor2GeoFeatureID(String actor2GeoFeatureID)
    {
        this.actor2GeoFeatureID = actor2GeoFeatureID;
    }

    public int getActionGeoType()
    {
        return actionGeoType;
    }

    public void setActionGeoType(int actionGeoType)
    {
        this.actionGeoType = actionGeoType;
    }

    public String getActionGeoFullName()
    {
        return actionGeoFullName;
    }

    public void setActionGeoFullName(String actionGeoFullName)
    {
        this.actionGeoFullName = actionGeoFullName;
    }

    public String getActionGeoCountryCode()
    {
        return actionGeoCountryCode;
    }

    public void setActionGeoCountryCode(String actionGeoCountryCode)
    {
        this.actionGeoCountryCode = actionGeoCountryCode;
    }

    public String getActionGeoADM1Code()
    {
        return actionGeoADM1Code;
    }

    public void setActionGeoADM1Code(String actionGeoADM1Code)
    {
        this.actionGeoADM1Code = actionGeoADM1Code;
    }

    public String getActionGeoADM2Code()
    {
        return actionGeoADM2Code;
    }

    public void setActionGeoADM2Code(String actionGeoADM2Code)
    {
        this.actionGeoADM2Code = actionGeoADM2Code;
    }

    public double getActionGeoLat()
    {
        return actionGeoLat;
    }

    public void setActionGeoLat(double actionGeoLat)
    {
        this.actionGeoLat = actionGeoLat;
    }

    public double getActionGeoLong()
    {
        return actionGeoLong;
    }

    public void setActionGeoLong(double actionGeoLong)
    {
        this.actionGeoLong = actionGeoLong;
    }

    public String getActionGeoFeatureID()
    {
        return actionGeoFeatureID;
    }

    public void setActionGeoFeatureID(String actionGeoFeatureID)
    {
        this.actionGeoFeatureID = actionGeoFeatureID;
    }

    public String getDateAdded()
    {
        return dateAdded;
    }

    public void setDateAdded(String dateAdded)
    {
        this.dateAdded = dateAdded;
    }

    public String getSourceUrl()
    {
        return sourceUrl;
    }

    public void setSourceUrl(String sourceUrl)
    {
        this.sourceUrl = sourceUrl;
    }

    public Date getEventDate()
    {
        return eventDate;
    }

    public void setEventDate(Date eventDate)
    {
        this.eventDate = eventDate;
    }

    @Override
    public String toString()
    {
        return "GdeltEventResource{" +
            "globalEventID=" + globalEventID +
            ", actor1Code='" + actor1Code + '\'' +
            ", actor1Name='" + actor1Name + '\'' +
            ", actor1CountryCode='" + actor1CountryCode + '\'' +
            ", actor1KnownGroupCode='" + actor1KnownGroupCode + '\'' +
            ", actor1EthnicCode='" + actor1EthnicCode + '\'' +
            ", actor1Religion1Code='" + actor1Religion1Code + '\'' +
            ", actor1Religion2Code='" + actor1Religion2Code + '\'' +
            ", actor1Type1Code='" + actor1Type1Code + '\'' +
            ", actor1Type2Code='" + actor1Type2Code + '\'' +
            ", actor1Type3Code='" + actor1Type3Code + '\'' +
            ", actor2Code='" + actor2Code + '\'' +
            ", actor2Name='" + actor2Name + '\'' +
            ", actor2CountryCode='" + actor2CountryCode + '\'' +
            ", actor2KnownGroupCode='" + actor2KnownGroupCode + '\'' +
            ", actor2EthnicCode='" + actor2EthnicCode + '\'' +
            ", actor2Religion1Code='" + actor2Religion1Code + '\'' +
            ", actor2Religion2Code='" + actor2Religion2Code + '\'' +
            ", actor2Type1Code='" + actor2Type1Code + '\'' +
            ", actor2Type2Code='" + actor2Type2Code + '\'' +
            ", actor2Type3Code='" + actor2Type3Code + '\'' +
            ", isRootEvent=" + isRootEvent +
            ", eventCode='" + eventCode + '\'' +
            ", eventBaseCode='" + eventBaseCode + '\'' +
            ", eventRootCode='" + eventRootCode + '\'' +
            ", quadClass=" + quadClass +
            ", goldsteinScale=" + goldsteinScale +
            ", numMentions=" + numMentions +
            ", numSources=" + numSources +
            ", numArticles=" + numArticles +
            ", avgTone=" + avgTone +
            ", actor1GeoType=" + actor1GeoType +
            ", actor1GeoFullName='" + actor1GeoFullName + '\'' +
            ", actor1GeoCountryCode='" + actor1GeoCountryCode + '\'' +
            ", actor1GeoADM1Code='" + actor1GeoADM1Code + '\'' +
            ", actor1GeoADM2Code='" + actor1GeoADM2Code + '\'' +
            ", actor1GeoLat=" + actor1GeoLat +
            ", actor1GeoLong=" + actor1GeoLong +
            ", actor1GeoFeatureID='" + actor1GeoFeatureID + '\'' +
            ", actor2GeoType=" + actor2GeoType +
            ", actor2GeoFullName='" + actor2GeoFullName + '\'' +
            ", actor2GeoCountryCode='" + actor2GeoCountryCode + '\'' +
            ", actor2GeoADM1Code='" + actor2GeoADM1Code + '\'' +
            ", actor2GeoADM2Code='" + actor2GeoADM2Code + '\'' +
            ", actor2GeoLat=" + actor2GeoLat +
            ", actor2GeoLong=" + actor2GeoLong +
            ", actor2GeoFeatureID='" + actor2GeoFeatureID + '\'' +
            ", actionGeoType=" + actionGeoType +
            ", actionGeoFullName='" + actionGeoFullName + '\'' +
            ", actionGeoCountryCode='" + actionGeoCountryCode + '\'' +
            ", actionGeoADM1Code='" + actionGeoADM1Code + '\'' +
            ", actionGeoADM2Code='" + actionGeoADM2Code + '\'' +
            ", actionGeoLat=" + actionGeoLat +
            ", actionGeoLong=" + actionGeoLong +
            ", actionGeoFeatureID='" + actionGeoFeatureID + '\'' +
            ", dateAdded='" + dateAdded + '\'' +
            ", sourceUrl='" + sourceUrl + '\'' +
            ", eventDate=" + eventDate +
            '}';
    }

}
