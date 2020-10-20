package ETLPipeline.DirParser;

import java.io.File;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.shaded.minlog.Log;
import org.nd4j.linalg.util.ArrayUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ETLPipeline.EventExtractStep;
import EventGraph.EventGraphMining;
import util.csv.GDELTReturnResult;

public class ProcessByThreeMonths {
	/**
	 * start the tensor processing?
	 */
	public static boolean openCorrelated = false;
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessByThreeMonths.class);

	/**
	 * tensor calculate
	 */
	private static final boolean openTensor = false;
	
	public static void main(String[] args) {
		ProcessByThreeMonths test = new ProcessByThreeMonths();
		test.mainProcessor();
	}
	
	
	/**
	 * entry point
	 */
	public void mainProcessor() {
		 		
		int[] Start = {2015,01,01,0,0};//{2015,01,01,0,0};
		int interval = 3;
		int amount = 13;
		//process the data
		processGdeltDataSet(Start,interval,amount);
		
	}
	
	/**
	 *  year the year to represent, from MIN_YEAR to MAX_YEAR
		month the month-of-year to represent, from 1 (January) to 12 (December)
		dayOfMonth the day-of-month to represent, from 1 to 31
		hour the hour-of-day to represent, from 0 to 23
		minute the minute-of-hour to represent, from 0 to 59
		 e.g. 2015 01 01 0 0 2018 12 31 23 59
	 */
	public List<String[]> dateIntervals(int[] Start,int interval, int amount){
		//year, month, dayOfMonth, hour, minute
		List<String[]> out = Lists.newArrayList();
		String separator="$"; //special
		//start time		
		LocalDateTime StartTime = LocalDateTime.of(Start[0],Start[1],Start[2],Start[3],Start[4]);
		while(amount>0) {
			LocalDateTime EndTime = StartTime.plusMonths(interval);
			int[] one= {
					StartTime.getYear(),StartTime.getMonthValue(),StartTime.getDayOfMonth(),StartTime.getHour(),StartTime.getMinute(),		
					EndTime.getYear(),EndTime.getMonthValue(),EndTime.getDayOfMonth(),EndTime.getHour(),EndTime.getMinute()
			};
			//get the string array
			String[] oneRec = new String[one.length];
			for(int i=0;i<oneRec.length;i++) {
				oneRec[i] = Integer.toString(one[i]);
			}
			
			logger.info("Date: "+StringUtils.join(oneRec, separator));
			
			out.add(oneRec);
			//adjust
			StartTime =EndTime ;
			amount --;
		}
		return out;
	}
	/**
	 * from 2015 to 2018 
	 * divided by three months;
	 * e.g. 2015 01 01 0 0 2018 12 31 23 59
	 */
	public void processGdeltDataSet(int[] Start,int interval, int amount) {
		
		//etl
		ETLDirApp testETL = new ETLDirApp();
		
		//graph build
		EventGraphMining testGM = new EventGraphMining();
		
		//home dir
				String homeDir = "/home/eric/trace" ;
				//gdelt dir
				String  gdeltDir = "gdelt";
				
				//date pair
				int datePairLen = 2*ETLDirApp.DateEntryNum;

				//extract the intervals
				List<String[]> ranges = dateIntervals(Start,interval, amount);
				
				Iterator<String[]> ier = ranges.iterator();
				while(ier.hasNext()) {
					//date interval
					String[] one = ier.next();
					
					logger.info("Date: "+Arrays.toString(one));
					//parse date
					String[] datePair = ETLDirApp.getSubStrings(one,0,datePairLen);
					
					String outPostfix = StringUtils.join(datePair);
					
					logger.info("Date: "+outPostfix);
					
					
					String outJSON = outPostfix+ETLDirApp.jsonPostfix;
					/**
					 * start the tensor processing?
					 */
					if(openCorrelated) {
					
					File tmpFile = new File(outJSON);
					
					if(!tmpFile.exists()) {
						List<File> dataFile = testETL.dirParser2Files(homeDir, gdeltDir, datePair);
						
						logger.info("Fed Files: "+dataFile.size()+"date: "+Arrays.toString(datePair));
						
						testETL.EventETTest2JSON(outJSON,dataFile);
					}
					
					logger.info("parsed JSON!");
					
					//load json
					GDELTReturnResult result = testETL.readJSONGDELTReturnResult(true,outJSON);
					
					logger.info("loaded JSON! " + result.getGdeltEventList().size());
					
					String tensorFormat = testETL.EventETExtractTestV0(result,outPostfix);
					if(openTensor) {
					//compute tensor
					String training = outPostfix;
					String[] tensorSize=tensorFormat.split(" ");
					String tensorDirOutput= training+ETLDirApp.Tensor;
					String M = "4";
					String N = "4";
					String K = "20";
					//S2 build the tensor model
					int startIndex=0;
					//training  output M  N K [I1] [I2] ... [IN]
					String[] args= {
							training,tensorDirOutput,M,N,K									
					};
					//combine array
					String[] TensorArgs=ArrayUtil.combine(args,tensorSize);
					//calculate tensor		
					testETL.EPTestTensorFactor(TensorArgs,startIndex,outPostfix);
					logger.info("Tensor completed!");
					}
					//release the json data
					tmpFile.delete();
					}
					
					//S3 build the graph
					//input file
					String transFile = outPostfix+ETLDirApp.EventItemsIndex;
					
					String similarityFile = null;
					//write to graph file
					String outputFile = outPostfix+ETLDirApp.GEXF;
					//similarity threshold
					float threshold = 0;
					//reverse index
					String reverseIndexFile = outPostfix+ETLDirApp.reversePostfix;
					
					 Map<Integer,String> indexNodeTable = Maps.newHashMap();
					 Map<Integer,String> indexActionTable = Maps.newHashMap();
					 Map<Integer,Date> indexDateTable = Maps.newHashMap();
					 //parse the event files
					 EventExtractStep.parseIndexJSON(reverseIndexFile,  
							 indexNodeTable,indexActionTable, indexDateTable);
					 logger.info("Items: entities: "+indexNodeTable.size()+
							 ", actions: "+indexActionTable.size()+
							 ", dates: "+indexDateTable.size());
					
					 //only event 
					boolean IsSimilarityOrNot = false;
					//action + actor
					boolean actorOnly = false;
					//build the graph
					testGM.buildGraph(IsSimilarityOrNot, transFile,similarityFile, 
							outputFile, threshold,
							indexNodeTable,indexActionTable,indexDateTable,actorOnly);
					logger.info("Graph completed!");
					
					
					
					
					
				}//parse each file
				
				
				
				//int nextStarter = 3+datePairLen;
				
				//current index
				//int currentIndex = 3+ datePairLen;
				
				//parse the file
				
								
		
	}
}
