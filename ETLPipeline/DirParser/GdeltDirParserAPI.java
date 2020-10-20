package ETLPipeline.DirParser;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

import util.web.DefaultGdeltConfiguration;
import util.web.GdeltConfiguration;
import util.web.GdeltDefaultDirectoryFileFactory;
import util.web.GdeltFileNameFormatter;
import util.web.GdeltLastUpdateUnzipper;

/**
 * read each file in the given dir
 * @author quanyongf
 *
 */
public class GdeltDirParserAPI {
	private static final Logger logger = LoggerFactory.getLogger(GdeltDirParserAPI.class);
	
	 private final GdeltConfiguration gdeltConfiguration;
	 private final GdeltFileNameFormatter gdeltFileNameFormatter;
	 private final GdeltLastUpdateUnzipper gdeltLastUpdateUnzipper;
	 /**
	  * constructor
	  */
	 public GdeltDirParserAPI(){
		 this.gdeltConfiguration = new DefaultGdeltConfiguration();		
		 this.gdeltFileNameFormatter = new GdeltFileNameFormatter(gdeltConfiguration);
		 this.gdeltLastUpdateUnzipper = new GdeltLastUpdateUnzipper();
	 }
	 
	/**
	 * gdelt dir
	 * @param homeDir
	 * @param gdeltDir
	 * @return
	 */
    public static File getDefaultGdeltParserDirectory(String homeDir,String  gdeltDir)
    {
        //return new File(System.getProperty(homeDir) + File.separator +  gdeltDir);
    	
    	return new File(homeDir + File.separator +  gdeltDir);
    }
    

	
	/**
	 * Obtain file list
	 * @param parentDestinationDir
	 * @param since
	 * @param until
	 * @return
	 */
	public List<File> parseFileWithinDateTime(File parentDestinationDir,LocalDateTime since, LocalDateTime until) {
		 
		
		//parent dir
		 File destinationDir = new File(parentDestinationDir.getAbsolutePath());
		 
		 LocalDateTime time = since;
		 List<File> lf = new ArrayList<File>();
		 
		 logger.info("Parent dir: "+destinationDir.toString()+", "+since.toString()+":"+until.toString());
		 
	        while (!time.isAfter(until))
	        {
	        	
	        	int year  = time.getYear();
	        	int month = time.getMonth().getValue();
	        	int dayOfMonth = time.getDayOfMonth();
	        	//int hour = time.getHour();
	        	//int minute =time.getMinute();
	        	
	        	File tmpFile = null;
	        	
	        	
	        			
	        			//GdeltDefaultDirectoryFileFactory.
	        			//getDirectory(parentDestinationDir, year, month, dayOfMonth);
	            if (destinationDir.exists())
	            {
	                File[] files = destinationDir.listFiles();
	                if (files != null)
	                {
	                    List<String> csvFileNames = Arrays.stream(files)
	                        .filter(File::isFile)
	                        .map(File::getName)
	                        .filter(n -> n.endsWith(".CSV"))
	                        .collect(Collectors.toList());

	                    // we found the csv, just return it
	                    String csvFileName = gdeltFileNameFormatter.formatGdeltCsvFilename(year, month, dayOfMonth);
	                    if (csvFileNames.contains(csvFileName))
	                    {
	                        logger.info("Found CSV file for: {}", csvFileName);
	                    	tmpFile= new File(destinationDir.getAbsolutePath() + File.separator + csvFileName);
	                    }

	                    List<String> zippedCsvFileNames = Arrays.stream(files)
	                        .filter(File::isFile)
	                        .map(File::getName)
	                        .filter(n -> n.endsWith(".CSV.zip"))
	                        .collect(Collectors.toList());

	                    // we found the zipped csv, just unzip it and return the unzipped file
	                    String zippedCsvFileName = gdeltFileNameFormatter.formatGdeltZippedCsvFilename(year, month, dayOfMonth);
	                    if (zippedCsvFileNames.contains(zippedCsvFileName))
	                    {
	                        logger.debug("Found zipped CSV file for: {}", zippedCsvFileName);
	                        File zippedCsv = new File(destinationDir.getAbsolutePath() + File.separator + zippedCsvFileName);
	                        tmpFile = unzipCsv(zippedCsv, false);
	                    }
	                }
	            }
	        	 
	            if(tmpFile!=null) {
	            	lf.add(tmpFile);
	            }
	        	//forward to next file
	        	 time = time.plusDays(1);//.plusMinutes(15);
	        }
	        
	        return lf;
	}
	
	
	
	public List<File> parseFileLocalFileSystemWithinDateTime(File parentDestinationDir,
			LocalDateTime since, LocalDateTime until) {
		 LocalDateTime time = since;
		 
		 List<File> lf = new ArrayList<File>();
		 
	        while (!time.isAfter(until))
	        {
	        	
	        	int year  = time.getYear();
	        	int month = time.getMonth().getValue();
	        	int dayOfMonth = time.getDayOfMonth();
	        	int hour = time.getHour();
	        	int minute =time.getMinute();
	        	
	        	File tmpFile = null;
	        	
	        	File destinationDir = GdeltDefaultDirectoryFileFactory.getDirectory(
	        			parentDestinationDir, year, month, dayOfMonth);
	            if (destinationDir.exists())
	            {
	                File[] files = destinationDir.listFiles();
	                if (files != null)
	                {
	                    List<String> csvFileNames = Arrays.stream(files)
	                        .filter(File::isFile)
	                        .map(File::getName)
	                        .filter(n -> n.endsWith(".CSV"))
	                        .collect(Collectors.toList());

	                    // we found the csv, just return it
	                    String csvFileName = gdeltFileNameFormatter.formatGdeltCsvFilename(year, month, dayOfMonth, hour, minute);
	                    if (csvFileNames.contains(csvFileName))
	                    {
	                        //logger.debug("Found CSV file for: {}", csvFileName);
	                    	tmpFile= new File(destinationDir.getAbsolutePath() + File.separator + csvFileName);
	                    }

	                    List<String> zippedCsvFileNames = Arrays.stream(files)
	                        .filter(File::isFile)
	                        .map(File::getName)
	                        .filter(n -> n.endsWith(".CSV.zip"))
	                        .collect(Collectors.toList());

	                    // we found the zipped csv, just unzip it and return the unzipped file
	                    String zippedCsvFileName = gdeltFileNameFormatter.formatGdeltZippedCsvFilename(year, month, dayOfMonth, hour, minute);
	                    if (zippedCsvFileNames.contains(zippedCsvFileName))
	                    {
	                        //logger.debug("Found zipped CSV file for: {}", zippedCsvFileName);
	                        File zippedCsv = new File(destinationDir.getAbsolutePath() + File.separator + zippedCsvFileName);
	                        tmpFile = unzipCsv(zippedCsv, false);
	                    }
	                }
	            }
	        	 
	            if(tmpFile!=null) {
	            	lf.add(tmpFile);
	            }
	        	//
	        	 time = time.plusMinutes(15);
	        }
	        
	        return lf;
	}
	
	
	
	 private File unzipCsv(File zippedCsvFile, boolean deleteZip)
	    {
	        return gdeltLastUpdateUnzipper.unzip(zippedCsvFile, deleteZip);
	    }
	 
}
