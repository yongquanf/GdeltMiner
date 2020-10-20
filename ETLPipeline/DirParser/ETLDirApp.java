package ETLPipeline.DirParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Date;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ETLPipeline.EventExtractStep;
import ETLPipeline.EventProcessStep;
import util.Cluster.MultDimData;
import util.Cluster.clusterChoice;
import util.Util.CosineSimilarity;
import util.Util.GraphStreamUtil;
import util.associationMining.AnalysisResult;
import util.csv.GDELTReturnResult;
import util.models.GdeltEventResource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSONWriter;
import com.alibaba.fastjson.serializer.*;
import com.google.common.collect.Sets;


public class ETLDirApp {

	private static final Logger logger = LoggerFactory.getLogger(ETLDirApp.class);
	
	//public static  String fileTest="";
	GdeltDirParserAPI dirParser;
	EventProcessStep ep;
	EventExtractStep et;
	
	/**
	 * list of event recs
	 */
	public final static String jsonPostfix = "_GDELTReturnResultJson";
	
	/**
	 * name-index map
	 */
	public final static String reversePostfix="_ReverseIndex";
	/**
	 * event from to date tuples
	 */
	public final static String  EventItemsIndex ="_EventItemsIndex";
	//event similarity files
	public final static String  outEventSimPutFile="_outEventSimPutFile";
	/**
	 * entry
	 */
	public final static int DateEntryNum = 5;

	public static final String GEXF = ".gml";

	public static final String Tensor = "Tensor";
	
	/**
	 * constructor of the parser
	 */
	public ETLDirApp() {
	
	 dirParser = new GdeltDirParserAPI();
	 ep = new  EventProcessStep();
	 et = new EventExtractStep();
	
	}
	/**
	 * time instance
	 * year the year to represent, from MIN_YEAR to MAX_YEAR
		month the month-of-year to represent, from 1 (January) to 12 (December)
		dayOfMonth the day-of-month to represent, from 1 to 31
		hour the hour-of-day to represent, from 0 to 23
		minute the minute-of-hour to represent, from 0 to 59
	 * @param year
	 * @param month
	 * @param dayOfMonth
	 * @param hour
	 * @param minute
	 * @return
	 */
	public static  LocalDateTime buildTime(List<Integer> time) {
		//len not enough
		if(time.size()<DateEntryNum) {
			return null;
		}else {
			int year = time.get(0);
			int month = time.get(1);
			int dayOfMonth = time.get(2);
			int hour = time.get(3);
			int minute = time.get(4);
			return  LocalDateTime.of(year, month, dayOfMonth, hour, minute);
		}
	}
	    
	/**
	 * date pair
	 * @param datePair
	 * @return
	 */
	public static  List<Integer> parseSinceUntilPairDate(List<String> datePair) {
		List<Integer> items = new ArrayList<Integer>();
		for(int i=0;i<datePair.size();i++) {
			items.add(Integer.parseInt(datePair.get(i)));
		}
		 return items;
	}
	
	/**
	 * since, until, each needs five fields, year, month, day, hour, minute
	 * @param datePair
	 * @return
	 */
	public static List<LocalDateTime> getDatePair(String[] datePair){
		List<LocalDateTime> builder = new ArrayList<LocalDateTime>();
		
		//parse the integer representation
		List<Integer> items =parseSinceUntilPairDate(Arrays.asList(datePair));
	
		
		//since		
		LocalDateTime first = buildTime(items.subList(0, DateEntryNum));
		//until
		LocalDateTime second = buildTime(items.subList(DateEntryNum, DateEntryNum*2));
		builder.add(first);
		builder.add(second);
		return builder;
	}
	
	/**
	 * parent directory
	 * @param homeDir
	 * @param gdeltDir
	 * @return
	 */
	private File getParentDirectory(String homeDir,String  gdeltDir) {
		return dirParser.getDefaultGdeltParserDirectory(homeDir, gdeltDir);
	}
	
	/**
	 * select file
	 * @param parent
	 * @param time, 0, since, 1, until
	 * @return
	 */
	private List<File> selectFiles(File parent, List<LocalDateTime> time ){
		return dirParser.parseFileWithinDateTime(parent, time.get(0),time.get(1));
	}
	
	/**
	 * main entry for the dir parser
	 * @param homeDir
	 * @param gdeltDir
	 * @param datePair
	 * @param lenPerTimer
	 * @return
	 */
	public List<File> dirParser2Files(String homeDir,
			String  gdeltDir,String[] datePair){
		
		//get the dates
		List<LocalDateTime> dates = getDatePair(datePair);
		
		File tmp = getParentDirectory(homeDir, gdeltDir);
		
		logger.info("home: "+homeDir+", "+gdeltDir);
		
		//select the files
		List<File> files= selectFiles(tmp,dates);
		
		return files;
	}
	
	static String dateFormat0="yyyy-MM-dd HH:mm:ss";
	/**
	 * serialize
	 */
	private static SerializeConfig mapping = new SerializeConfig();
	static {
		
		mapping.put(Date.class,new SimpleDateFormatSerializer(dateFormat0));
	}
	/**
	 * write to json
	 * 05-23
	 * @param result
	 * @param outPostfix
	 */
	public void write2JSON(GDELTReturnResult result, String outJSONPostfix) {
		
		//String jsonString = JSONObject.toJSONString(result);
		try {
			//FileWriter output = new FileWriter(new File(outJSONPostfix));
			//JSON.writeJSONString(output, result);
			//output.close();
			JSONWriter writer = new JSONWriter (new FileWriter(
					new File(outJSONPostfix)));
			writer.startArray();			
			for(int i=0;i<result.getGdeltEventList().size();i++) {
				//writer.writeObject(result.getGdeltEventList().get(i));
				writer.writeObject(FastJsonUtil.bean2Json(result.getGdeltEventList().get(i)));
			}
			//writer.writeObject(result.getDownloadResult());
			writer.endArray();
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * reader the json object
	 * @param inJSONPostfix
	 * @return
	 */
	public GDELTReturnResult readJSONGDELTReturnResult(boolean iscluster,
			String inJSONPostfix) {
		try {
			
			GDELTReturnResult out = new GDELTReturnResult();
			
			 JSONReader reader = new JSONReader(new FileReader(
					 new File(inJSONPostfix)));
			 
			 logger.info(inJSONPostfix+", "+reader.toString());
			 
			 try {
			 int index = 0;
			reader.startArray();
			while(reader.hasNext()) {
				if(index%10000==0) {
					logger.info("items: "+out.getGdeltEventList().size());
				}
				//parse the json object
				
				String str = reader.readString();
					GdeltEventResource one = FastJsonUtil.
						json2Bean(str,GdeltEventResource.class);//reader.readObject(GdeltEventResource.class);
				
				 GdeltEventResource gdeltEvent =  new GdeltEventResource();
				 assignGdeltRec(iscluster,one,gdeltEvent);
				
				//store to the list
				out.getGdeltEventList().add(gdeltEvent);
				index++;
				
			}
			reader.endArray();
			reader.close();
			
			 }catch(JSONException e){
					return out;
			}
				
				
			
			//InputStream in = new FileReader(new File(inJSONPostfix));
			return out;
			//GDELTReturnResult reader=JSON.parseObject(in,GDELTReturnResult.class);
			//return reader;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}
	
	/**
	 * reduce the data, half
	 * @param from
	 * @param gdeltEvent
	 */
	private void assignGdeltRec(boolean iscluster,GdeltEventResource x, GdeltEventResource gdeltEvent) {
		// TODO Auto-generated method stub
		
		if(!iscluster) {
		gdeltEvent.setGlobalEventID(x.getGlobalEventID());
		gdeltEvent.setActor1Name(x.getActor1Name());		
		gdeltEvent.setActor2Name(x.getActor2Name());
		
		gdeltEvent.setEventBaseCode(x.getEventBaseCode());
		gdeltEvent.setEventDate( x.getEventDate());
		gdeltEvent.setNumMentions(x.getNumMentions());
		}else {
			gdeltEvent.setGlobalEventID(x.getGlobalEventID());
			gdeltEvent.setActor1Name(x.getActor1Name());		
			gdeltEvent.setActor2Name(x.getActor2Name());
			gdeltEvent.setActor1CountryCode(x.getActor1CountryCode());
			gdeltEvent.setActor2CountryCode(x.getActor2CountryCode());
			
			gdeltEvent.setEventBaseCode(x.getEventBaseCode());
			gdeltEvent.setEventDate( x.getEventDate());
		gdeltEvent.setActor1GeoLat(x.getActor1GeoLat());
		gdeltEvent.setActor1GeoLong( x.getActor1GeoLong());
		
		gdeltEvent.setActor2GeoLat(x.getActor2GeoLat());
		gdeltEvent.setActor2GeoLong( x.getActor2GeoLong());
		
		gdeltEvent.setActionGeoLat(x.getActionGeoLat());
		gdeltEvent.setActionGeoLong( x.getActionGeoLong());
		
		gdeltEvent.setAvgTone( x.getAvgTone());
		gdeltEvent.setGoldsteinScale( x.getGoldsteinScale());
		 
		gdeltEvent.setNumMentions( x.getNumMentions());
		gdeltEvent.setNumSources( x.getNumSources());
		gdeltEvent.setNumArticles( x.getNumArticles());
		}
		 
	}
	private GdeltEventResource slim0(GdeltEventResource one) {
		// TODO Auto-generated method stub

			// TODO Auto-generated method stub
			 String empty = "";
			 one.setActor1KnownGroupCode(empty);		 
			 one.setActor1EthnicCode(empty);
			 one.setActor1Religion1Code(empty);
			 one.setActor1Religion2Code(empty);
			 one.setActor1Type1Code(empty);
			 one.setActor1Type2Code(empty);
			 one.setActor1Type3Code(empty);
			
			 one.setActor2KnownGroupCode(empty);		 
			 one.setActor2EthnicCode(empty);
			 one.setActor2Religion1Code(empty);
			 one.setActor2Religion2Code(empty);
			 one.setActor2Type1Code(empty);
			 one.setActor2Type2Code(empty);
			 one.setActor2Type3Code(empty);
			 
			 one.setSourceUrl(empty);
			 
			 one.setActionGeoADM1Code(empty);
			 one.setActionGeoADM2Code(empty);
			 one.setActionGeoFeatureID(empty);
			 
			 one.setActor1GeoADM1Code(empty);
			 one.setActor1GeoADM2Code(empty);
			 one.setActor1GeoFeatureID(empty);
			 
			 one.setActor2GeoADM1Code(empty);
			 one.setActor2GeoADM2Code(empty);
			 one.setActor2GeoFeatureID(empty);
		
		return one;
	}
	/**
	 * 
	 * 
	 * @param args
	 */
	public static void main(String[] args){
		
		ETLDirApp test = new ETLDirApp();
		
		printError();
		
		//extract
		int selector = Integer.valueOf(args[0]);
		
		//home dir
		String homeDir = args[1];
		//gdelt dir
		String  gdeltDir = args[2];
		
		//date pair
		int datePairLen = 2*DateEntryNum;
		/**
		 *  year the year to represent, from MIN_YEAR to MAX_YEAR
			month the month-of-year to represent, from 1 (January) to 12 (December)
			dayOfMonth the day-of-month to represent, from 1 to 31
			hour the hour-of-day to represent, from 0 to 23
			minute the minute-of-hour to represent, from 0 to 59
		 */
		String[] datePair = getSubStrings(args,3,datePairLen);
		
		int nextStarter = 3+datePairLen;
		
		//current index
		//int currentIndex = 3+ datePairLen;
		
		//parse the file
		
		
		String outPostfix = StringUtils.join(datePair);
		
		String outJSON = outPostfix+jsonPostfix;
		
		//extract tensor
		if(selector==1){
					    
			File tmpFile = new File(outJSON);
			
			if(!tmpFile.exists()) {
				List<File> dataFile = test.dirParser2Files(homeDir, gdeltDir, datePair);
				
				logger.info("Fed Files: "+dataFile.size()+"date: "+Arrays.toString(datePair));
				
				test.EventETTest2JSON(outJSON,dataFile);
			}
			//load json
			GDELTReturnResult result = test.readJSONGDELTReturnResult(false,outJSON);
			
			String format = test.EventETExtractTestV0(result,outPostfix);
			
			if(false) {
				test.write2JSON(result,outPostfix+jsonPostfix);
			}
			if(format!=null){
				logger.info("Tensor: "+format);
			}else{
				logger.error("tensor failed!");
			}
		//mining events	
		}else if(selector==2){
			int starter = nextStarter;
			 
			GDELTReturnResult result = test.
					readJSONGDELTReturnResult(true,outJSON);//outPostfix+jsonPostfix //test.EventETTest(dataFile);
			
			//double minSupport, double minConfidence,int kmeansPPClusterNum
			double minSupport = Double.valueOf(args[starter]);
			double minConfidence = Double.valueOf(args[starter+1]);
			int kmeansPPClusterNum = Integer.valueOf(args[starter+2]);
			//event components
			test.EPTestAssociationAndCluster(minSupport, 
					minConfidence, kmeansPPClusterNum,result,outPostfix);
		//mine the tensor
		}else if(selector==3){//create tensor factorization
			//[0 training  output M  N K [I1] [I2] ... [IN]]
			//event represent
			int startIndex = nextStarter + 3;
			test.EPTestTensorFactor(args,startIndex,outPostfix);
		}else if(selector ==4) {//create similarity
			
			int start = nextStarter + 3;
			
			 String training4TensorFactorization = args[start];
			 String factorFileDir = args[start+1];
			 String M = args[start+2];
			 
			int N = Integer.valueOf(args[start +3]);
			int K = Integer.valueOf(args[start + 4]);
			 
			 //I1,I2,I3,I4
			 int[] len = new int[Integer.valueOf(N)];
			 for(int i=0;i<len.length;i++){
				 len[i] = Integer.valueOf(args[start+5+i]);
			 }
			 
			 //event-itemset index
			 String EventItemIndexFile = 
					 outPostfix+EventItemsIndex;//"_EventItemsIndex";
			//out
			String outEventSimPutFileDir=factorFileDir+outPostfix+
					File.separator+ outEventSimPutFile;
			
			//event similarity analysis
			test.SimpleEventCorrelationAnalysis(factorFileDir+outPostfix, 
					EventItemIndexFile, outEventSimPutFileDir,
					N, K, len);
		}else if(selector ==5) {
			//fit the missing matrix, input: 
			//(i,j,val), index i, index j, shortest-path-length
			
			int start = nextStarter + 3;
			//training
			 String training4TensorFactorization = args[start];
			 String factorFileDir = args[start+1]+"FitMat";
			 String M = args[start+2];
			//2-d matrix 
			int N = 2;
			String dimension = "2";
			//rank
			int K = Integer.valueOf(args[start +4]);
			String rank = args[start +4]; 
			 //I1,I2,I3,I4
			 int[] len = new int[Integer.valueOf(N)];
			 for(int i=0;i<len.length;i++){
				 len[i] = Integer.valueOf(args[start+5+i]);
			 }
			 
			 String[] lenStr = new String[Integer.valueOf(dimension)];
			 for(int i=0;i<len.length;i++){
				 lenStr[i] = args[start+5+i];
			 }	
			 
			 //compute the factor matrix
			 //training iterations
			 String Tout = "20";
			 //inner cycles
			 String Tin = "10";
			 //column
			 String C = "1";
			 //weighted tensor
			 String useWeight ="0";
			 //sparse regularization
			 String lambda ="0.1";
			 String lambda4bias ="0.1";
			 //assign tensor
			 String[] tensorParams = new String[11+len.length];
			 test.assignParams(tensorParams,training4TensorFactorization,factorFileDir,
					 M,Tout,Tin,dimension,rank,C,useWeight,lambda,lambda4bias,lenStr);
			 //estimate
			 boolean useBias = false;			 		 
			 test.ep.tensorFactor(tensorParams, useBias);
			
			 //fill missing value
			 String EventItemIndexFile = training4TensorFactorization+
					 outPostfix+EventItemsIndex;//"_EventItemsIndex";
			 
			String outEventSimPutFileDir =  "FitMat"+factorFileDir+outPostfix+
					File.separator+ outEventSimPutFile;
			try {
				fitMissingMatrixByTensor(factorFileDir, 
						EventItemIndexFile, outEventSimPutFileDir,
						N, K, len);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * fit the missing matrix
	 * @param factorFileDir
	 * @param eventItemIndexFile
	 * @param outEventSimPutFileDir
	 * @param n
	 * @param k
	 * @param len
	 * @throws Exception 
	 */
	private static void fitMissingMatrixByTensor(String factorFileDir, 
			String EventItemIndexFile,
			String outPutFile, int dim, int rank, int[] rowSizes) throws Exception {
		// TODO Auto-generated method stub
		//step 1, load event item index;
		String delim = ",";
		BufferedReader br = new BufferedReader(new FileReader(EventItemIndexFile));
		
		//event map
		Set<Integer> allKeys = Sets.newHashSet();
		while(true){
			String line = br.readLine();
			if(line==null)
				break;		
			List<Integer> items = EventProcessStep.extractEventItemList(line,delim);
			//add the indexes
			allKeys.addAll(items);
		}
		br.close();
		
		//step 2, load factor matrix
		//[n],[I_i],[K]
		BufferedWriter outSimilarity = new BufferedWriter(new FileWriter(outPutFile));
		//load the factor files
		float[][][] params=EventProcessStep.loadParamMatrices(factorFileDir,rowSizes,dim,rank);
				
		
		
		Iterator<Integer> ier1 = allKeys.iterator();
		while(ier1.hasNext()) {
			
			int me = ier1.next();
			
		//step 3, for each pair of event, compute the similarity
		Iterator<Integer> ier = allKeys.iterator();
		while(ier.hasNext()) {
			Integer you = ier.next();
			//indices for the tensor
			int[] indices= {me,you};
			//compute
			float predict = 0;
			for(int k=0; k<rank; k++){
				float product = 1;
				for(int n=0; n<dim; n++){
					product *= params[n][indices[n]][k];
				}
				predict += product;
			}
			//predicted value 			
			outSimilarity.append(me+","+you +","+predict+"\n");
		}
		
		}//end
		outSimilarity.flush();
		outSimilarity.close();
		
		//step 4, Return the list of similarity from the selected file to the other files
		
		allKeys.clear();
		params = null;
	}
	/**
	 * output of the etl pipeline
	 */
	 private static void printError() {
	        System.err.println("java ETLDirApp selector homeDir  gdeltDir datePair,..."
	        		+ "minSupport minConfidence kmeansPPClusterNum "
	        		+ "training4TensorFactorization factorFileDir"  
	        		+ "M N K  {I1,I2,I3,I4,...}");
	        System.err.println("selector: "+"[1,2,3,4]"+"1: extract tensor"+", 2: Association mining And k-means Cluster "+", 3: calculate tensor factor by sals"+", 4: create event similarity file");
	        System.err.println("homeDir  gdeltDir: "+"directory to be parsed");
	        System.err.println("datePair: "+ "[from until], e.g., each date is a 5-tuple:[year month dayOfMonth hour minute]");
	        System.err.println("minSupport minConfidence "+" association mining threshold");
	        System.err.println("kmeansPPClusterNum" + "k-means++ clustering cluster number");
	        System.err.println("training4TensorFactorization "+"tensor factorization training data");
	        System.err.println("factorFileDir "+" factor file output dir"); 
	        System.err.println("M "+"number of cores");
	        System.err.println("N "+"dimension of the tensor, I1,...I4");
	        System.err.println("K "+"tensor rank");
	        System.err.println("{I1,I2,I3,I4,...} "+"factor size"); 
	        System.err.println("kmeansPPCluster output: "+" association mining result, k-means clustering (action, actor1,actor2,eventMensions, eventToneGoldstein)"
	        		+ " stored in EPTestAssociationAndCluster");
	        System.err.println("tensor-factorization output: "+" factor file");
	        System.err.println("tensor-factorization similarity output: "+ "EventItemIndexFile + outEventSimPutFile");
	 }
	
	
	
	
	/**
	 * get string array
	 * @param args
	 * @param i
	 * @param j
	 * @return
	 */
	public static String[] getSubStrings(String[] args, int from, int len) {
		// TODO Auto-generated method stub
		int to = from + len;
		List<String> list = Arrays.asList(args);
		if(list!=null) {
			return getArrayFromList(list.subList(from, to));
		}else {
			return null;
		}
								
	}
	
	/**
	 * return the array
	 * @param subList
	 * @return
	 */
	public static String[] getArrayFromList(List<String> subList) {
		// TODO Auto-generated method stub
		if(subList!=null&&!subList.isEmpty()) {
			return subList.toArray(new String[subList.size()]);
		}else {
			return null;
		}
	}
	/**
	 * extract
	 * @param dataFile 
	 * @return
	 */
	public GDELTReturnResult EventETTest0(List<File> files){
		
		GDELTReturnResult all=null;
		EventExtractStep et = new EventExtractStep();
		//reader
		for(File item: files) {
			
			logger.info("Process: "+item.toString());
			
			GDELTReturnResult gresult = et.readCSVTrace(item);
			//parsed
			logger.info("parsed: "+gresult.getGdeltEventList().size());
			
			if(all ==null) {
				all = gresult;
			}else {
				all.merge(gresult);
				gresult.clear();
			}
			//
			 if(all!=null) {
				 
					logger.info(all.toString()+"\n");
					logger.info(all.getDownloadResult().toString()+"\n");
					logger.info(all.getGdeltEventList().size()+"\n");
			}
		}
		
		
		//extract to tensor
		//et.Extract4dTensor(gresult, "fileTestTensor");
		return all;
	}
	
	/**
	 * batch mode
	 * @param outJSONPostfix
	 * @param files
	 * @return
	 */
	public void EventETTest2JSON(String outJSONPostfix, List<File> files){
		
		GDELTReturnResult all=null;
		EventExtractStep et = new EventExtractStep();
		
		JSONWriter writer;
		try {
			writer = new JSONWriter (new FileWriter(
					new File(outJSONPostfix)));
			
			writer.startArray();	
			
			for(File item: files) {
			
				logger.info("Process: "+item.getName());
				
				GDELTReturnResult gresult = et.readCSVTrace(item);
				//write to json
			for(int i=0;i<gresult.getGdeltEventList().size();i++) {
				//writer.writeObject(result.getGdeltEventList().get(i));
				writer.writeObject(FastJsonUtil.bean2Json(gresult.getGdeltEventList().get(i)));
			}
			
			gresult.clear();
			writer.flush();
			
			}
			
			//writer.writeObject(result.getDownloadResult());
			writer.endArray();
			writer.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
//		//reader
//		for(File item: files) {
//			
//			logger.info("Process: "+item.toString());
//			
//			GDELTReturnResult gresult = et.readCSVTrace(item);
//			//parsed
//			logger.info("parsed: "+gresult.getGdeltEventList().size());
//			
//			if(all ==null) {
//				all = gresult;
//			}else {
//				all.merge(gresult);
//				gresult.clear();
//			}
//			//
//			 if(all!=null) {
//				 
//					logger.info(all.toString()+"\n");
//					logger.info(all.getDownloadResult().toString()+"\n");
//					logger.info(all.getGdeltEventList().size()+"\n");
//			}
//		}
		
		
		//extract to tensor
		//et.Extract4dTensor(gresult, "fileTestTensor");
		//return all;
	}
	
	
	/**
	 * extract the event
	 * @param gresult
	 */
	public String  EventETExtractTestV1(GDELTReturnResult gresult){
		
		
		//read
		//GDELTReturnResult gresult = et.readCSVTrace(fileTest);
		//logger.info(gresult.toString()+"\n");
		//logger.info(gresult.getDownloadResult().toString()+"\n");
		//logger.info(gresult.getGdeltEventList().size()+"\n");
		//extract to tensor
		return et.Extract4dTensorV0(gresult, "fileTestTensor");
		 
	}
	
	/**
	 * global index
	 * @param gresult
	 * @param postfix
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 * @param format
	 * @return
	 */
	public String  EventETExtractTest(GDELTReturnResult gresult,
			String postfix,
			Map<String,Integer> indexNodeTable,
			Map<String,Integer> indexActionTable,
			Map<java.util.Date,Integer> indexDateTable,
			String format){

		
		//read
		//GDELTReturnResult gresult = et.readCSVTrace(fileTest);
		//logger.info(gresult.toString()+"\n");
		//logger.info(gresult.getDownloadResult().toString()+"\n");
		//logger.info(gresult.getGdeltEventList().size()+"\n");
		//extract to tensor
		return et.Extract4dTensor(gresult, postfix,indexNodeTable,
				indexActionTable,indexDateTable,format);
		 
	}

	/**
	 * original
	 * @param gresult
	 * @param postfix
	 * @return
	 */
	public String  EventETExtractTestV0(GDELTReturnResult gresult,String postfix){
		
		
		//read
		//GDELTReturnResult gresult = et.readCSVTrace(fileTest);
		//logger.info(gresult.toString()+"\n");
		//logger.info(gresult.getDownloadResult().toString()+"\n");
		//logger.info(gresult.getGdeltEventList().size()+"\n");
		//extract to tensor
		return et.Extract4dTensorV0(gresult, postfix);
		 
	}
	
	/**
	 * process the returned
	 * @param minSupport
	 * @param minConfidence
	 * @param kmeansPPClusterNum
	 * @param result
	 * @param outPostfix 
	 */
	public void EPTestAssociationAndCluster(double minSupport, double minConfidence,
			int kmeansPPClusterNum,GDELTReturnResult result, String outPostfix){
		 
		FileWriter output;
		try {
			String outFileName = StringUtils.join("EPTestAssociationAndCluster",outPostfix);
		   //append
			output = new FileWriter(new File(outFileName), true);
			output.append("#Params: "+minSupport+" "+minConfidence+" "+kmeansPPClusterNum+" trace: "+result.getGdeltEventList().size()+"\n");
			
			
		 //association rule mining
		 //double minSupport = 0.05;
		// double minConfidence = 0.5;
		 AnalysisResult associationRules = ep.frequentItemsSearch(result, minSupport, minConfidence);
		 output.append("Association: "+associationRules.toString()+"\n");
		 
		 //k-means clustering
		 //int kmeansPPClusterNum = 5;
		 int maxIters = 100;
		 List<CentroidCluster<MultDimData>> cResult = ep.clusterBySingleDimension(result, clusterChoice.action, kmeansPPClusterNum, maxIters);
		 writeClusterResult(output,clusterChoice.action,cResult);
		 
		 cResult = ep.clusterBySingleDimension(result, clusterChoice.actor1, kmeansPPClusterNum, maxIters);
		 writeClusterResult(output,clusterChoice.actor1,cResult);
		 
		 cResult = ep.clusterBySingleDimension(result, clusterChoice.actor2, kmeansPPClusterNum, maxIters);		
		 writeClusterResult(output,clusterChoice.actor2,cResult);
		 
		 cResult = ep.clusterBySingleDimension(result, clusterChoice.eventMensions, kmeansPPClusterNum, maxIters);
		 writeClusterResult(output,clusterChoice.eventMensions,cResult);
		 
		 cResult = ep.clusterBySingleDimension(result, clusterChoice.eventToneGoldstein, kmeansPPClusterNum, maxIters);
		 writeClusterResult(output,clusterChoice.eventToneGoldstein,cResult);
		 
		 
		 output.flush();
		 output.close();
		}catch(Exception e){e.printStackTrace();}
				 
	}
	
	/**
	 * perform similarity on the pairwise events
	 * @param factorFileDir
	 * @param EventItemIndexFile
	 * @param outPutFile
	 * @param dim
	 * @param rank
	 * @param rowSizes
	 */
	public void SimpleEventCorrelationAnalysis(String factorFileDir, 
			String EventItemIndexFile, String outEventSimPutFile, 
			int dim, int rank, int[] rowSizes) {
		try {
			this.ep.similarityAnalysis(factorFileDir, EventItemIndexFile, 
					outEventSimPutFile, dim, rank, rowSizes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * write the cluster result
	 * @param output
	 * @param action
	 * @param cResult
	 * @throws IOException 
	 */
	public static void writeClusterResult(FileWriter output, clusterChoice action, 
			List<CentroidCluster<MultDimData>> cResult) throws IOException {
		// TODO Auto-generated method stub
		if(action!=null) {
			output.write("choice "+action.name());
		}
		for(CentroidCluster<MultDimData> e:cResult){
			output.append("ClusterCentroid: " + Arrays.toString(e.getCenter().getPoint())+", "+"\n");
			//output.append("Cluster: " + StringUtils.join(e.getPoints(),",")+"\n");
		}
		for(CentroidCluster<MultDimData> e:cResult){
			//output.append("ClusterCentroid: " + Arrays.toString(e.getCenter().getPoint())+", "+"\n");
			output.append("Cluster: " + StringUtils.join(e.getPoints(),",")+"\n");
		}
		output.flush();
	}

	/**
	 * param training	training data
	 * param output output file
	 * param N dimension
	 * param K	rank
	 * param Tout	number of outer iterations
	 * param Tin	number of inner iterations
	 * param M	number of cores
	 * param C	number of columns updated at a time
	 * param lambda	regularization parameter
	 * param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * param useBias	whether to use bias terms
	 * param start index of parameter value
	 * @param outPostfix 
	 */
	public void EPTestTensorFactor(String[] args,int start, String outPostfix){
		 //tensor factor, 4-order,
		 //(actor,actor,action,month): counts
		 //run_single_bias_sals.sh
		 //[training] [output] [M] [Tout] [Tin] [N] [K] [C] [useWeight] [lambda]  [I1] [I2] ... [IN] [test] [query]
		
		 //training  output M  N K [I1] [I2] ... [IN]
		
		 String training = args[start];
		 String output = StringUtils.join(args[start+1],outPostfix);
		 String M = args[start+2];
		 
		 String dimension =args[start +3];
		 String rank = args[start + 4];
		 
		 //I1,I2,I3,I4
		 String[] len = new String[Integer.valueOf(dimension)];
		 for(int i=0;i<len.length;i++){
			 len[i] = args[start+5+i];
		 }		
		 //training iterations
		 String Tout = "40";
		 //inner cycles
		 String Tin = "10";
		 //column
		 String C = "1";
		 //weighted tensor
		 String useWeight ="0";
		 //sparse regularization
		 String lambda ="0.1";
		 String lambda4bias ="0.1";
		 
		 String[] tensorParams = new String[11+len.length];
		 assignParams(tensorParams,training,output,
				 M,Tout,Tin,dimension,rank,C,useWeight,lambda,lambda4bias,len);
		 
		 boolean useBias = true;
		 		 
		 ep.tensorFactor(tensorParams, useBias);
	}

	/**
	 * assign parameters by order
	 * @param tensorParams
	 * @param training
	 * @param output
	 * @param m
	 * @param tout
	 * @param tin
	 * @param n
	 * @param k
	 * @param c
	 * @param useWeight
	 * @param lambda
	 * @param len
	 */
	private void assignParams(String[] tensorParams, String training, 
			String output, String m, String tout, String tin,
			String n, String k, String c, String useWeight, 
			String lambda, String lambda4bias, String[] len) {
		// TODO Auto-generated method stub
		tensorParams[0]=training;
		tensorParams[1]=output;
		tensorParams[2]=m;
		tensorParams[3]=tout;
		tensorParams[4]=tin;
		tensorParams[5]=n;
		tensorParams[6]=k;
		tensorParams[7]=c;
		tensorParams[8]=useWeight;
		tensorParams[9]=lambda;
		tensorParams[10]=lambda4bias;
		
		for(int i=0;i<len.length;i++){
			tensorParams[11+i]=len[i];
		}
	}
}
