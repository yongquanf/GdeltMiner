package EventCorrelationMining;

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
import ETLPipeline.DirParser.ETLDirApp;
import EventGraph.EventGraphMining;
import util.csv.GDELTReturnResult;

public class ProcessByThreeMonthsPlus {
	/**
	 * start the tensor processing?
	 */
    public static String homeDir = "/home/XXX" ; //Gdelt downloaded parent directory	
    public static String  gdeltDir = "gdelt"; //gdelt directory name
	public static boolean createIndex = false;
	public static boolean openCorrelated = true;
	public static boolean openGraph = false;
	
	public static boolean openFactorTopK = false;
	
	private static final Logger logger = LoggerFactory.getLogger(ProcessByThreeMonthsPlus.class);
	private static final boolean openTensor = false;
	
	public static void main(String[] args) {
		ProcessByThreeMonthsPlus test = new ProcessByThreeMonthsPlus();
		test.mainProcessor();
	}
	
	/**
	 * 2020,01,16, generate dictionary, for later parser
	 */
	public List<String> generateDictionary(int[] Start,int interval, int amount) {
		//etl
				ETLDirApp testETL = new ETLDirApp();
				
				//graph build
				EventGraphMining testGM = new EventGraphMining();
				
				//home dir
				String homeDir = homeDir;
						//gdelt dir
				String  gdeltDir = gdeltDir;
				//time
				String[] first =null;
				String[] last =null;
				
				/**
				 * extract index in global scale
				 */
				EventExtractStep et = new EventExtractStep();
				/**
				 * init parameters
				 */
				Map<String,Integer> indexNodeTable = Maps.newHashMap();
				Map<String,Integer> indexActionTable = Maps.newHashMap();
				Map<Date,Integer> indexDateTable = Maps.newHashMap();
				int indexNode =0;
				int indexAction=0;
				int indexDate=0;
				
						
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
							
							File tmpFile = new File(outJSON);
							
							if(!tmpFile.exists()) {
								List<File> dataFile = testETL.
										dirParser2Files(homeDir, gdeltDir, datePair);
								
								logger.info("Fed Files: "+dataFile.size()+"date: "+Arrays.toString(datePair));
								
								testETL.EventETTest2JSON(outJSON,dataFile);
							}
							
							logger.info("parsed JSON!");
							
							//load json
							GDELTReturnResult result = testETL.readJSONGDELTReturnResult(false,outJSON);
							//invalid
							if(result==null||result.getGdeltEventList().isEmpty()) {
								continue;
							}
							//update once
							if(first==null) {
								first = datePair;
							}
							//always update
							last = datePair;
							
							logger.info("loaded JSON! " + result.getGdeltEventList().size());
							
							//testETL.EventETExtractTest(result,outPostfix);
							
							List<Integer> indexList = et.ExtractDictionaryFromTensor(
									result,
									outPostfix,
									indexNodeTable,
									indexActionTable,
									indexDateTable,
									indexNode,
									indexAction,
									indexDate);
							
							//update index
							indexNode = indexList.get(0);
							indexAction = indexList.get(1);
							indexDate = indexList.get(2);
							
							//reset
							result.clear();
							
						}//update
						
						//write
						String outFileName =  StringUtils.join(first).concat( StringUtils.join(last));
						
						String format = et.StoreDictionary(outFileName,
								indexNodeTable,
								indexActionTable,
								indexDateTable);
						
						//tensor format, index file
						List<String> list = Lists.newArrayList();
						list.add(format);
						list.add(outFileName);
						return list;
	}
	
	/**
	 * entry point
	 */
	public void mainProcessorV0() {
		 		
		int[] Start = {2014,01,01,0,0}; //{2018,01,01,0,0};//{2015,01,01,0,0};
		int interval = 6;
		int amount = 20;
		//process the data
		processGdeltDataSetV0(Start,interval,amount);
		
	}
	
	public void mainProcessor() {
 		
		int[] Start = {2014,01,01,0,0}; //{2018,01,01,0,0};//{2015,01,01,0,0};
		int interval = 1;//3;
		int amount = 20;
		
		String format="";
		String indexFile="";
		if(createIndex) {
		//process the data
			List<String> list = generateDictionary(Start,interval,amount);
			format= list.get(0);
			indexFile = list.get(1);
		}else
		{
			List<String> list = loadFromFile();
			format= list.get(0);
			indexFile = list.get(1);
		}
		
		processGdeltDataSet(format,indexFile,Start,interval,amount);
		
	}
	
	/**
	 * load format and file from file
	 * @return
	 */
	private List<String> loadFromFile() {
		// TODO Auto-generated method stub
		String formatFile = "201441002014710020181010020191100TensorFormat";
		String indexFile = "201441002014710020181010020191100";
		
		List<String> list = Lists.newArrayList();
		list.add(formatFile);
		list.add(indexFile);
		return list;
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
	 * index file
	 * @param format
	 * @param IndexFile
	 * @param Start
	 * @param interval
	 * @param amount
	 */
	public void processGdeltDataSet(String format, String IndexFile,
			int[] Start,int interval, int amount) {
		
		
		 
		/**
		 * reverse the index
		 */
		 Map<String,Integer> indexNodeTableD = Maps.newHashMap();
		 Map<String,Integer> indexActionTableD = Maps.newHashMap();
		 Map<Date,Integer> indexDateTableD = Maps.newHashMap();
		 
		 if(false) {
		 //parse the event files
		 EventExtractStep.parseIndexJSOND(IndexFile+ETLDirApp.reversePostfix,  
				 indexNodeTableD,indexActionTableD, indexDateTableD);
		 }
		
		
		//etl
		ETLDirApp testETL = new ETLDirApp();
		
		//graph build
		EventGraphMining testGM = new EventGraphMining();
		
		//home dir
				String homeDir = homeDir; // "/home/eric/trace" ;
				//gdelt dir
				String  gdeltDir = gdeltDir;//"gdelt";
				
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
					String training = outPostfix;
					String tensorDirOutput= training+ETLDirApp.Tensor;
					String M = "4";
					String N = "4";
					String K = "20";
					
					/**
					 * start the tensor processing?
					 */
					if(openCorrelated) {
					
					File tmpFile = new File(outJSON);
					
					if(!tmpFile.exists()) {
						List<File> dataFile = testETL.
								dirParser2Files(homeDir, gdeltDir, datePair);
						
						logger.info("Fed Files: "+dataFile.size()+"date: "+Arrays.toString(datePair));
						
						testETL.EventETTest2JSON(outJSON,dataFile);
					}
					
					logger.info("parsed JSON!");
					
					//load json
					GDELTReturnResult result = testETL.readJSONGDELTReturnResult(false,outJSON);
					
					logger.info("loaded JSON! " + result.getGdeltEventList().size());
					
					/**
					 * parse tensor
					 */
					String tensorFormat = testETL.EventETExtractTest(
							result,
							outPostfix,
							indexNodeTableD,
							indexActionTableD,
							indexDateTableD,
							format);
					//reset
					//result.clear();
					
					if(openTensor) {
					//compute tensor
					
					String[] tensorSize=tensorFormat.split(" ");
					
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
					
					//ecminer
					int topK = 20;


					
					//S3 build the graph
					//input file
					String transFile = outPostfix+ETLDirApp.EventItemsIndex;
					
					String similarityFile = null;
					//write to graph file
					String outputFile = outPostfix+ETLDirApp.GEXF;
					//similarity threshold
					float threshold = 0;
					//reverse index
					String reverseIndexFile = IndexFile+ETLDirApp.reversePostfix; //outPostfix+ETLDirApp.reversePostfix;
					

					
					 if(openFactorTopK) {
						 
						 Map<Integer,String> indexNodeTable = Maps.newHashMap();
						 Map<Integer,String> indexActionTable = Maps.newHashMap();
						 Map<Integer,Date> indexDateTable = Maps.newHashMap();
						 //parse the event files
						 EventExtractStep.parseIndexJSON(reverseIndexFile,  
								 indexNodeTable,indexActionTable, indexDateTable);
						 logger.info("Items: entities: "+indexNodeTable.size()+
								 ", actions: "+indexActionTable.size()+
								 ", dates: "+indexDateTable.size());
						 
					//mining the tensor factors
					 String factorFileDir = outPostfix+"Tensor"+outPostfix;
						String outEventTopKPutFile=factorFileDir+File.separator+ 
								outPostfix+ecMiner.outEventTopKAttributeFile;
						
						TopKAttribeQuery tk = new TopKAttribeQuery();
						try {
							tk.topKAnalysis(factorFileDir, transFile, 
									outEventTopKPutFile, Integer.valueOf(N), Integer.valueOf(K),
									topK,indexNodeTable,
									indexActionTable,indexDateTable);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						logger.info("Topk completed!");
					 }
						
						
					 if(openGraph) {
					 //build the graph
					 //only event 
						 Map<Integer,String> indexNodeTable = Maps.newHashMap();
						 Map<Integer,String> indexActionTable = Maps.newHashMap();
						 Map<Integer,Date> indexDateTable = Maps.newHashMap();
						 //parse the event files
						 EventExtractStep.parseIndexJSON(reverseIndexFile,  
								 indexNodeTable,indexActionTable, indexDateTable);
						 logger.info("Items: entities: "+indexNodeTable.size()+
								 ", actions: "+indexActionTable.size()+
								 ", dates: "+indexDateTable.size());	 
						 
					boolean IsSimilarityOrNot = false;
					//action + actor
					boolean actorOnly = true;
					//build the graph
					testGM.buildGraph(IsSimilarityOrNot, transFile,similarityFile, 
							outputFile, threshold,
							indexNodeTable,indexActionTable,indexDateTable,actorOnly);
					logger.info("Graph completed!");
					 }
					
					//store file
					//incrementally
					EventExtractStep.StoreDictionary(outPostfix+ETLDirApp.reversePostfix,
								indexNodeTableD,
								indexActionTableD,
								indexDateTableD);
					
				}//parse each file
				

		
	}
	
	/**
	 * from 2015 to 2018 
	 * divided by three months;
	 * e.g. 2015 01 01 0 0 2018 12 31 23 59
	 */
	public void processGdeltDataSetV0(int[] Start,int interval, int amount) {
		
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
					String training = outPostfix;
					String tensorDirOutput= training+ETLDirApp.Tensor;
					String M = "4";
					String N = "4";
					String K = "20";
					
					/**
					 * start the tensor processing?
					 */
					if(openCorrelated) {
					
					File tmpFile = new File(outJSON);
					
					if(!tmpFile.exists()) {
						List<File> dataFile = testETL.
								dirParser2Files(homeDir, gdeltDir, datePair);
						
						logger.info("Fed Files: "+dataFile.size()+"date: "+Arrays.toString(datePair));
						
						testETL.EventETTest2JSON(outJSON,dataFile);
					}
					
					logger.info("parsed JSON!");
					
					//load json
					GDELTReturnResult result = testETL.readJSONGDELTReturnResult(false,outJSON);
					
					logger.info("loaded JSON! " + result.getGdeltEventList().size());
					
					String tensorFormat = testETL.EventETExtractTestV0(result,outPostfix);
					
					if(openTensor) {
					//compute tensor
					
					String[] tensorSize=tensorFormat.split(" ");
					
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
					
					//ecminer
					int topK = 20;

					 
					
					 
					
					
					
					
					
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
					
					 if(openFactorTopK) {
					//mining the tensor factors
					 String factorFileDir = outPostfix+"Tensor"+outPostfix;
						String outEventTopKPutFile=factorFileDir+File.separator+ 
								outPostfix+ecMiner.outEventTopKAttributeFile;
						
						TopKAttribeQuery tk = new TopKAttribeQuery();
						try {
							tk.topKAnalysis(factorFileDir, transFile, 
									outEventTopKPutFile, Integer.valueOf(N), Integer.valueOf(K),
									topK,indexNodeTable,
									indexActionTable,indexDateTable);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						logger.info("Topk completed!");
					 }
						
						
					 if(openGraph) {
					 //build the graph
					 //only event 
					boolean IsSimilarityOrNot = false;
					//action + actor
					boolean actorOnly = true;
					//build the graph
					testGM.buildGraph(IsSimilarityOrNot, transFile,similarityFile, 
							outputFile, threshold,
							indexNodeTable,indexActionTable,indexDateTable,actorOnly);
					logger.info("Graph completed!");
					 }
					
					
					
					
				}//parse each file
				
				
				
				//int nextStarter = 3+datePairLen;
				
				//current index
				//int currentIndex = 3+ datePairLen;
				
				//parse the file
				
								
		
	}
}
