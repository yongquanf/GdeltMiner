package util.models;

import java.io.Serializable;

/**
 * @author Kevin Chen
 */
public class GdeltDownloadResultResource implements Serializable
{
    private boolean downloadedSuccessfully;

    private int recordsLoaded;

    private int recordsFailed;
    
    public GdeltDownloadResultResource() {
    	downloadedSuccessfully = false;
    	recordsLoaded = 0;
    	recordsFailed = 0;
    }

    public boolean getDownloadedSuccessfully()
    {
        return downloadedSuccessfully;
    }

    public void setDownloadedSuccessfully(boolean downloadedSuccessfully)
    {
        this.downloadedSuccessfully = downloadedSuccessfully;
    }

    public int getRecordsLoaded()
    {
        return recordsLoaded;
    }

    public void setRecordsLoaded(int recordsLoaded)
    {
        this.recordsLoaded = recordsLoaded;
    }

    public int getRecordsFailed()
    {
        return recordsFailed;
    }

    public void setRecordsFailed(int recordsFailed)
    {
        this.recordsFailed = recordsFailed;
    }

	public void merge(GdeltDownloadResultResource downloadResult) {
		// TODO Auto-generated method stub
		this.recordsLoaded+=downloadResult.recordsLoaded;
		this.recordsFailed+=downloadResult.recordsFailed;
	}
}
