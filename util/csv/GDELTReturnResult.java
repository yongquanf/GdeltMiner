package util.csv;

import util.models.GdeltDownloadResultResource;
import util.models.GdeltEventResource;

import java.util.List;

import org.apache.commons.compress.utils.Lists;

/**
 * @author Kevin Chen
 */
public class GDELTReturnResult
{
    private GdeltDownloadResultResource downloadResult;

    private List<GdeltEventResource> gdeltEventList;

    public GDELTReturnResult() {
    	gdeltEventList = Lists.newArrayList();
    	downloadResult = new GdeltDownloadResultResource();
    }
    
    public GdeltDownloadResultResource getDownloadResult()
    {
        return downloadResult;
    }

    public void setDownloadResult(GdeltDownloadResultResource downloadResult)
    {
        this.downloadResult = downloadResult;
    }

    public List<GdeltEventResource> getGdeltEventList()
    {
        return gdeltEventList;
    }

    public void setGdeltEventList(List<GdeltEventResource> gdeltEventList)
    {
        this.gdeltEventList = gdeltEventList;
    }
    
    /**
     * merge other
     * @param other
     */
    public void merge(GDELTReturnResult other) {
    	if(other!=null&&!other.gdeltEventList.isEmpty()) {
    		this.gdeltEventList.addAll(other.getGdeltEventList());
    		this.getDownloadResult().merge(other.getDownloadResult());
    	}
    }

	public void clear() {
		// TODO Auto-generated method stub
		this.gdeltEventList.clear();
		this.gdeltEventList=null;
	}
    
}
