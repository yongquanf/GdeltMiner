package ETLPipeline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.YearMonth;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSONWriter;
import com.google.common.collect.Lists;

import ETLPipeline.DirParser.ETLDirApp;
import util.Util.FourpleGP;
import util.csv.GDELTReturnResult;
import util.csv.GdeltCSVParser;
import util.models.GdeltEventResource;

public class EventExtractStep {
	private static final Logger logger = LoggerFactory.getLogger(EventExtractStep.class);
	
	/**
	 * reader file into the memory list
	 */
	GdeltCSVParser reader;
	
	/**
	 * step0, constructor
	 */
	public EventExtractStep(){
		reader = new GdeltCSVParser();
	}
	
	/**
	 * step 1.reader
	 * @param fileName
	 */
	public GDELTReturnResult readCSVTrace(String fileName){
		return reader.parseCsv(new File(fileName));
	}
	
	public GDELTReturnResult readCSVTrace(File fileName){
		return reader.parseCsv(fileName);
	}
	
	
	/**
	 * extract dictionary
	 * @param trace
	 * @param outFileName
	 * @return
	 */
	public List<Integer> ExtractDictionaryFromTensor(GDELTReturnResult trace,
			String outFileName,
			Map<String,Integer> indexNodeTable,
			Map<String,Integer> indexActionTable,
			Map<Date,Integer> indexDateTable,
			int indexNode,
			int indexAction,
			int indexDate){
		
		/**
		 * return index
		 */
		List<Integer> indexes = Lists.newArrayList();
		
		/**
		 * 1 pass, extract actor1, actor2, action list, date interval.
		 */
		
		logger.info("trace size: "+trace.getGdeltEventList().size());
		
		//int indexNode=0;
		//int indexAction=0;
		//int indexDate=0;
		//entity index
		//Map<String,Integer> indexNodeTable = new HashMap<String,Integer>();
		//Map<String,Integer> indexActionTable = new HashMap<String,Integer>();
		 
		//date index, aggregate by month by default
		//Map<Date,Integer> indexDateTable = new HashMap<Date,Integer>();
		Date DateFirst = null;
		
		Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		while(ier0.hasNext()){
			GdeltEventResource x = ier0.next();
			//skip
			if(x.getActor1Name().isEmpty()||x.getActor2Name().isEmpty()) {
				//logger.warn("Empty actor detect and skipped");
				continue;
			}
			
			//logger.info(x.toString());
			//actor
			String tmp = x.getActor1Name();
			//logger.info("actor1: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
			tmp = x.getActor2Name();
			//logger.info("actor2: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
									
			//event type
			String tmp2 = x.getEventBaseCode();
			//logger.info("eventBase: "+tmp2);
			if(!indexActionTable.containsKey(tmp2)){
				indexActionTable.put(tmp2, indexAction);
				indexAction++;
			}
			
			
			//
			Date time = x.getEventDate();
			//date
			if(!indexDateTable.containsKey(time)){
				indexDateTable.put(time, indexDate);
				 indexDate++;
			}
			
			
		}//end index
		
		indexes.add( indexNode);
		indexes.add(indexAction);
		indexes.add(indexDate);
		return indexes;
	}
	
	/**
	 * store to dictionary
	 * @param outFileName
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 * @return
	 */
	public static String StoreDictionary(String outFileName, Map<String,Integer> indexNodeTable,
			Map<String,Integer> indexActionTable,
			Map<Date,Integer> indexDateTable){

		/**
		 * 2 pass, produce 4-tuple items
		 */
		
		FileWriter outputIndex;
		try {
			
			outputIndex = new FileWriter(new File(outFileName+ETLDirApp.reversePostfix));
			String outputIdx = outFileName+ETLDirApp.EventItemsIndex;
			//FileWriter outputIndex2 = new FileWriter(new File(outputIdx));
			
			//storeIndexes(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			storeIndexesJSON(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			outputIndex.flush();
			outputIndex.close();
					
		logger.info("cache: "+indexNodeTable.size()+", "+indexActionTable.size()+
				", "+indexDateTable.size());
		
		
		FileWriter output0;
		output0 = new FileWriter(new File(outFileName+"TensorFormat"));
		String format = (indexNodeTable.size())+" "+(indexNodeTable.size())+" "+ 
		(indexActionTable.size())+" "+(indexDateTable.size());
		output0.write(format+"\n");
		output0.flush();
		output0.close();
		
		//write the tuples
		//writeFourple(outputIdx,outFileName);
		
		

		
		return format;
		}catch(Exception e){e.printStackTrace();}
		
		return null;
	}
	
	/**
	 * write by global index
	 * @param trace
	 * @param outFileName
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 * @return
	 */
	public String Extract4dTensor(GDELTReturnResult trace,
			String outFileName,
			Map<String,Integer> indexNodeTable,
			Map<String,Integer> indexActionTable,
			Map<Date,Integer> indexDateTable,
			String format){
				
		/**
		 * 2 pass, produce 4-tuple items
		 */
		
		//FileWriter outputIndex;
		try {
			
			//outputIndex = new FileWriter(new File(outFileName+ETLDirApp.reversePostfix));
			String outputIdx = outFileName+ETLDirApp.EventItemsIndex;
			FileWriter outputIndex2 = new FileWriter(new File(outputIdx));
			
			//storeIndexes(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			//storeIndexesJSON(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			//outputIndex.flush();
			//outputIndex.close();
			

		
		logger.info("cache: "+indexNodeTable.size()+", "+indexActionTable.size()+
				", "+indexDateTable.size());
		
		//Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		//int maxFrom=-1;
		//int maxTo = -1;
		//int maxAction=-1;
		//int maxDate = -1;
		
		List<GdeltEventResource> traceList = trace.getGdeltEventList();
		
		//GdeltEventResource oneRec;
		for(GdeltEventResource oneRec:traceList){
			
			//GdeltEventResource oneRec = ier0.next();
			
			int globalcode =oneRec.getGlobalEventID();
			
			String from = oneRec.getActor1Name();
			String to = oneRec.getActor2Name();
			String action = oneRec.getEventBaseCode();
			Date dateNow = oneRec.getEventDate();
			
			int reported = oneRec.getNumMentions();
			
			//if( !indexNodeTable.containsKey(from)||!indexNodeTable.containsKey(to)){
			//	logger.warn("empty triple, skipped");
			//	continue;
			//}
			
			if(from.isEmpty()||to.isEmpty()||action.isEmpty()||dateNow==null) {
				logger.warn("empty triple, skipped");
				continue;
			}
			
			/**
			 * fill missing
			 */
			fillMissing(from,to,action,dateNow,indexNodeTable,indexActionTable,indexDateTable);
			
			//id
			int indexFrom = indexNodeTable.get(from);
			int indexTo = indexNodeTable.get(to);
			int indexEvent = indexActionTable.get(action);
			int indexDate0 = indexDateTable.get(dateNow).intValue();
			
			//logger.info(indexFrom+", "+indexTo+", "+indexEvent+", "+indexDate0);
			
			//index
			storeIndexes(outputIndex2,globalcode,indexFrom,indexTo,indexEvent,indexDate0,reported);
			
		}//output the event record
		
		outputIndex2.flush();
		outputIndex2.close();
		
		//trace.clear();

		FileWriter output0;
		output0 = new FileWriter(new File(outFileName+"TensorFormat"));
		int maxFrom = maxValue(indexNodeTable);
		int maxAction = maxValue(indexActionTable);
		int maxDate = maxValueDate(indexDateTable);
		String format0 = (maxFrom+1)+" "+(maxFrom+1)+" "+ (maxAction+1)+" "+(maxDate+1);
		output0.write(format+"\n");
		output0.flush();
		output0.close();
		
		//write the tuples
		writeFourple(outputIdx,outFileName);
		
		

		
		return format0;
		}catch(Exception e){e.printStackTrace();}
		
		int maxFrom = maxValue(indexNodeTable);
		int maxAction = maxValue(indexActionTable);
		int maxDate = maxValueDate(indexDateTable);
		String format0 = (maxFrom+1)+" "+(maxFrom+1)+" "+ (maxAction+1)+" "+(maxDate+1);
		
		return format0;
	}
	
	
	/**
	 * fill missing items
	 * @param from
	 * @param to
	 * @param action
	 * @param dateNow
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	private void fillMissing(String from, String to, String action, Date dateNow, 
			Map<String, Integer> indexNodeTable,
			Map<String, Integer> indexActionTable, 
			Map<Date, Integer> indexDateTable) {
		// TODO Auto-generated method stub
		
		if(!indexNodeTable.containsKey(from)){
			logger.warn("missing from: " +from);
			indexNodeTable.put(from, maxValue(indexNodeTable)+1);			
		}

		if(!indexNodeTable.containsKey(to)){
			logger.warn("missing to: " +to);
			indexNodeTable.put(to, maxValue(indexNodeTable)+1);
			
		}	
		//logger.info("eventBase: "+tmp2);
		if(!indexActionTable.containsKey(action)){
			logger.warn("missing action: " +action);
			indexActionTable.put(action, maxValue(indexActionTable)+1);
		}
		//
		//date
		if(!indexDateTable.containsKey(dateNow)){
			logger.warn("missing date: " +dateNow.toString());
			indexDateTable.put(dateNow, maxValueDate(indexDateTable)+1);			
		}
		
		
	}
	/**
	 * maxValue
	 * @param indexDateTable
	 * @return
	 */
	private int maxValueDate(Map<Date, Integer> indexDateTable) {
		// TODO Auto-generated method stub
		int maxVal = -1;
		if(indexDateTable.isEmpty()) {
			return -1;
		}
		Iterator<Integer> ier = indexDateTable.values().iterator();
		while(ier.hasNext()) {
			int val = ier.next();
			if(maxVal<val) {
				maxVal=val;
			}
		}
		return maxVal;
	}

	/**
	 * get max
	 * @param indexNodeTable
	 * @return
	 */
	private int maxValue(Map<String, Integer> indexNodeTable) {
		// TODO Auto-generated method stub
		int maxVal = -1;
		if(indexNodeTable.isEmpty()) {
			return -1;
		}
		Iterator<Integer> ier = indexNodeTable.values().iterator();
		while(ier.hasNext()) {
			int val = ier.next();
			if(maxVal<val) {
				maxVal=val;
			}
		}
		return maxVal;
	}

	/**
	 * transform list to tensor,
	 * 4-d tuple (i,j,k,l), 
	 * [0, max-1]
	 * write by line to file
	 * aggregate by month
	 * actor1,actor2,action,date
	 */
	public String Extract4dTensorV0(GDELTReturnResult trace,String outFileName){
		/**
		 * 1 pass, extract actor1, actor2, action list, date interval.
		 */
		
		logger.info("trace size: "+trace.getGdeltEventList().size());
		
		int indexNode=0;
		int indexAction=0;
		int indexDate=0;
		//entity index
		Map<String,Integer> indexNodeTable = new HashMap<String,Integer>();
		Map<String,Integer> indexActionTable = new HashMap<String,Integer>();
		 
		//date index, aggregate by month by default
		Map<Date,Integer> indexDateTable = new HashMap<Date,Integer>();
		Date DateFirst = null;
		
		Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		while(ier0.hasNext()){
			GdeltEventResource x = ier0.next();
			//skip
			if(x.getActor1Name().isEmpty()||x.getActor2Name().isEmpty()) {
				//logger.warn("Empty actor detect and skipped");
				continue;
			}
			
			//logger.info(x.toString());
			//actor
			String tmp = x.getActor1Name();
			//logger.info("actor1: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
			tmp = x.getActor2Name();
			//logger.info("actor2: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
			
			
			
			//event type
			String tmp2 = x.getEventBaseCode();
			//logger.info("eventBase: "+tmp2);
			if(!indexActionTable.containsKey(tmp2)){
				indexActionTable.put(tmp2, indexAction);
				indexAction++;
			}
			
			
			//
			Date time = x.getEventDate();
			//date
			if(!indexDateTable.containsKey(time)){
				indexDateTable.put(time, indexDate);
				 indexDate++;
			}
			if(false) {
			if(DateFirst==null){
				DateFirst = x.getEventDate();
				
				logger.info("date: "+DateFirst.toString());
				//first 0
				indexDateTable.put(DateFirst,0);
				 
			}else{
				Date newDate = x.getEventDate();
				//difference in month +1, as the index in the hash table
				logger.info("date: "+newDate.toString());	
				
				if(!indexDateTable.containsKey(newDate)){
					//compute the month difference
					long hourOffset=getHourIndex(newDate,DateFirst);//getDayIndex(newDate,DateFirst);		//getMonthIndex(newDate,DateFirst);		
					indexDateTable.put(newDate,(int)hourOffset); 
				}
			}
			}
			
		}//end index
		
		
		
		
		/**
		 * 2 pass, produce 4-tuple items
		 */
		
		FileWriter outputIndex;
		try {
			
			outputIndex = new FileWriter(new File(outFileName+ETLDirApp.reversePostfix));
			String outputIdx = outFileName+ETLDirApp.EventItemsIndex;
			FileWriter outputIndex2 = new FileWriter(new File(outputIdx));
			
			//storeIndexes(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			storeIndexesJSON(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			outputIndex.flush();
			outputIndex.close();
			

		
		logger.info("cache: "+indexNodeTable.size()+", "+indexActionTable.size()+
				", "+indexDateTable.size());
		
		//Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		int maxFrom=-1;
		int maxTo = -1;
		int maxAction=-1;
		int maxDate = -1;
		
		List<GdeltEventResource> traceList = trace.getGdeltEventList();
		
		//GdeltEventResource oneRec;
		for(GdeltEventResource oneRec:traceList){
			
			//GdeltEventResource oneRec = ier0.next();
			
			int globalcode =oneRec.getGlobalEventID();
			
			String from = oneRec.getActor1Name();
			String to = oneRec.getActor2Name();
			String action = oneRec.getEventBaseCode();
			Date dateNow = oneRec.getEventDate();
			
			int reported = oneRec.getNumMentions();
			
			if( !indexNodeTable.containsKey(from)||!indexNodeTable.containsKey(to)){
				//logger.warn("empty triple, skipped");
				continue;
			}
					
			//id
			int indexFrom = indexNodeTable.get(from);
			int indexTo = indexNodeTable.get(to);
			int indexEvent = indexActionTable.get(action);
			int indexDate0 = indexDateTable.get(dateNow).intValue();
			
			//logger.info(indexFrom+", "+indexTo+", "+indexEvent+", "+indexDate0);
			
			//index
			storeIndexes(outputIndex2,globalcode,indexFrom,indexTo,indexEvent,indexDate0,reported);

			//record maximum
			if(indexFrom>maxFrom){
				maxFrom = indexFrom;
			}
			if(indexTo>maxTo){
				maxTo = indexTo;
			}
			if(indexEvent>maxAction){
				maxAction = indexEvent;
			}
			if(indexDate0>maxDate){
				maxDate = indexDate0;
			}	
			
		}//output the event record
		
		outputIndex2.flush();
		outputIndex2.close();
		
		trace.clear();
		indexNodeTable.clear();
		indexActionTable.clear();
		indexDateTable.clear();
		
		FileWriter output0;
		output0 = new FileWriter(new File(outFileName+"TensorFormat"));
		String format = (maxFrom+1)+" "+(maxTo+1)+" "+ (maxAction+1)+" "+(maxDate+1);
		output0.write(format+"\n");
		output0.flush();
		output0.close();
		
		//write the tuples
		writeFourple(outputIdx,outFileName);
		
		

		
		return format;
		}catch(Exception e){e.printStackTrace();}
		
		return null;
	}
	
	
	/**
	 * parse event item index
	 * @param eventItemIndex
	 * @throws IOException 
	 */
	private void writeFourple(String eventItemIndex,String outFileName) throws IOException {
		// TODO Auto-generated method stub
		//parse the event record	
		
		String delim = ",";
        //int maxNum = 0;
        
    	Map<FourpleGP<Integer,Integer,Integer,Integer>,Integer> mapCounter = new 
				HashMap<FourpleGP<Integer,Integer,Integer,Integer>,Integer>();			
		
    	FileWriter output = new FileWriter(new File(outFileName));
        
        final BufferedReader br = new BufferedReader(new FileReader(eventItemIndex));
        while(true) {
            final String line = br.readLine();
            if(line == null) {
                break;
            }
            else if(line.startsWith("#") || line.startsWith("%") || line.startsWith("//")) { //comment
                continue;
            }
            else {
                String[] tokens = line.split(delim);
                int eventID = Integer.valueOf(tokens[0]);
                int actor1 = Integer.valueOf(tokens[1]);
                int actor2 = Integer.valueOf(tokens[2]);
                int action = Integer.valueOf(tokens[3]);
                int date  = Integer.valueOf(tokens[4]);
		

				//create fourple
				FourpleGP<Integer,Integer,Integer,Integer> one = FourpleGP.fourple(actor1,
						actor2,action,date);
				if(!mapCounter.containsKey(one)){
					mapCounter.put(one, 1);
				}else{
					//increase the counter
					mapCounter.computeIfPresent(one, 
							(FourpleGP<Integer,Integer,Integer,Integer> key, Integer val)->++val );
				}
				
            }
        }//end parse

        
				//iterate the map, write to the file, deliminated by ","
				Iterator<Entry<FourpleGP<Integer, Integer, Integer, Integer>, Integer>> ff = mapCounter.
						entrySet().iterator();
				while(ff.hasNext()){
					Entry<FourpleGP<Integer, Integer, Integer, Integer>, Integer> tmpFF = ff.next();
					output.write(tmpFF.getKey().asRecord()+","+tmpFF.getValue()+"\n");
					output.flush();
					
				}
				//clear
				mapCounter.clear();
				//indexActionTable.clear();
				//indexDateTable.clear();
				//indexNodeTable.clear();
				
				output.flush();
				output.close();
				

	}

	public String Extract4dTensorBasic(GDELTReturnResult trace,String outFileName){
		/**
		 * 1 pass, extract actor1, actor2, action list, date interval.
		 */
		
		logger.info("trace size: "+trace.getGdeltEventList().size());
		
		int indexNode=0;
		int indexAction=0;
		int indexDate=0;
		//entity index
		Map<String,Integer> indexNodeTable = new HashMap<String,Integer>();
		Map<String,Integer> indexActionTable = new HashMap<String,Integer>();
		 
		//date index, aggregate by month by default
		Map<Date,Integer> indexDateTable = new HashMap<Date,Integer>();
		Date DateFirst = null;
		
		Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		while(ier0.hasNext()){
			GdeltEventResource x = ier0.next();
			//skip
			if(x.getActor1Name().isEmpty()||x.getActor2Name().isEmpty()) {
				//logger.warn("Empty actor detect and skipped");
				continue;
			}
		 
			
			//logger.info(x.toString());
			//actor
			String tmp = x.getActor1Name();
			//logger.info("actor1: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
			tmp = x.getActor2Name();
			//logger.info("actor2: "+tmp);
			if(!indexNodeTable.containsKey(tmp)){
				indexNodeTable.put(tmp, indexNode);
				indexNode++;
			}
			//event type
			String tmp2 = x.getEventBaseCode();
			//logger.info("eventBase: "+tmp2);
			if(!indexActionTable.containsKey(tmp2)){
				indexActionTable.put(tmp2, indexAction);
				indexAction++;
			}
			//
			Date time = x.getEventDate();
			//date
			if(!indexDateTable.containsKey(time)){
				indexDateTable.put(time, indexDate);
				 indexDate++;
			}
			if(false) {
			if(DateFirst==null){
				DateFirst = x.getEventDate();
				
				logger.info("date: "+DateFirst.toString());
				//first 0
				indexDateTable.put(DateFirst,0);
				 
			}else{
				Date newDate = x.getEventDate();
				//difference in month +1, as the index in the hash table
				logger.info("date: "+newDate.toString());	
				
				if(!indexDateTable.containsKey(newDate)){
					//compute the month difference
					long hourOffset=getHourIndex(newDate,DateFirst);//getDayIndex(newDate,DateFirst);		//getMonthIndex(newDate,DateFirst);		
					indexDateTable.put(newDate,(int)hourOffset); 
				}
			}
			}
			
		}//end index
		
		
		
		
		/**
		 * 2 pass, produce 4-tuple items
		 */
		
		FileWriter output,outputIndex;
		try {
			output = new FileWriter(new File(outFileName));
			outputIndex = new FileWriter(new File(outFileName+ETLDirApp.reversePostfix));
			FileWriter outputIndex2 = new FileWriter(new File(outFileName+ETLDirApp.EventItemsIndex));
			
			//storeIndexes(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			storeIndexesJSON(outputIndex,indexNodeTable,indexActionTable,indexDateTable);
			outputIndex.flush();
			outputIndex.close();
			
		Map<FourpleGP<Integer,Integer,Integer,Integer>,Integer> mapCounter = new 
				HashMap<FourpleGP<Integer,Integer,Integer,Integer>,Integer>();			
		
		logger.info("cache: "+indexNodeTable.size()+", "+indexActionTable.size()+
				", "+indexDateTable.size());
		
		//Iterator<GdeltEventResource> ier0 = trace.getGdeltEventList().iterator();
		int maxFrom=-1;
		int maxTo = -1;
		int maxAction=-1;
		int maxDate = -1;
		
		List<GdeltEventResource> traceList = trace.getGdeltEventList();
		
		//GdeltEventResource oneRec;
		for(GdeltEventResource oneRec:traceList){
			
			//GdeltEventResource oneRec = ier0.next();
			
			int globalcode =oneRec.getGlobalEventID();
			
			String from = oneRec.getActor1Name();
			String to = oneRec.getActor2Name();
			String action = oneRec.getEventBaseCode();
			Date dateNow = oneRec.getEventDate();
			
			int reported = oneRec.getNumMentions();
			
			
			if( !indexNodeTable.containsKey(from)||!indexNodeTable.containsKey(to)){
				//logger.warn("empty triple, skipped");
				continue;
			}
			
			//id
			int indexFrom = indexNodeTable.get(from);
			int indexTo = indexNodeTable.get(to);
			int indexEvent = indexActionTable.get(action);
			int indexDate0 = indexDateTable.get(dateNow).intValue();
			
			//logger.info(indexFrom+", "+indexTo+", "+indexEvent+", "+indexDate0);
			
			//index
			storeIndexes(outputIndex2,globalcode,indexFrom,indexTo,indexEvent,indexDate0, reported );

			//record maximum
			if(indexFrom>maxFrom){
				maxFrom = indexFrom;
			}
			if(indexTo>maxTo){
				maxTo = indexTo;
			}
			if(indexEvent>maxAction){
				maxAction = indexEvent;
			}
			if(indexDate0>maxDate){
				maxDate = indexDate0;
			}
			
			//create fourple
			FourpleGP<Integer,Integer,Integer,Integer> one = FourpleGP.fourple(indexFrom,
					indexTo,indexEvent,indexDate0);
			if(!mapCounter.containsKey(one)){
				mapCounter.put(one, 1);
			}else{
				//increase the counter
				mapCounter.computeIfPresent(one, 
						(FourpleGP<Integer,Integer,Integer,Integer> key, Integer val)->++val );
			}
			
		}
		
		
		outputIndex2.flush();
		outputIndex2.close();
		
		//iterate the map, write to the file, deliminated by ","
		Iterator<Entry<FourpleGP<Integer, Integer, Integer, Integer>, Integer>> ff = mapCounter.
				entrySet().iterator();
		while(ff.hasNext()){
			Entry<FourpleGP<Integer, Integer, Integer, Integer>, Integer> tmpFF = ff.next();
			output.write(tmpFF.getKey().asRecord()+","+tmpFF.getValue()+"\n");
			
			
		}
		//clear
		mapCounter.clear();
		indexActionTable.clear();
		indexDateTable.clear();
		indexNodeTable.clear();
		
		output.flush();
		output.close();
		

		FileWriter output0;

			output0 = new FileWriter(new File(outFileName+"TensorFormat"));
			String format = "N:N:A:D: "+ (maxFrom+1)+" "+(maxTo+1)+" "+ (maxAction+1)+" "+(maxDate+1);
			output0.write(format+"\n");
			output0.flush();
			output0.close();
		
		return format;
		}catch(Exception e){e.printStackTrace();}
		
		return null;
	}

	/**
	 * 
	 * @param outputIndex
	 * @param globalcode
	 * @param indexFrom
	 * @param indexTo
	 * @param indexEvent
	 * @param indexDate
	 * @param reported
	 */
	private void storeIndexes(FileWriter outputIndex, int globalcode, int indexFrom, int indexTo, int indexEvent,
			int indexDate, int reported) {
		// TODO Auto-generated method stub
		try {
			outputIndex.append(globalcode+","+indexFrom+","+indexTo+","+indexEvent+","+indexDate+","+reported+"\n");
			outputIndex.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	/**
	 * parse the file
	 * @param jsonIndexFile
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public static void parseIndexJSON(String jsonIndexFile, Map<Integer,String> indexNodeTable,
			Map<Integer,String> indexActionTable, Map<Integer,Date> indexDateTable) {
		
		try {
			//parse the json file
			//JSONObject jsonObj = JSONObject.parseObject(jsonIndexFile);
			
			JSONReader reader = new JSONReader(new FileReader(jsonIndexFile));
			reader.startArray();
			//reader.startObject();
			//while(String key:jsonObj.keySet()) {
			while(reader.hasNext()) {
				
				String key = reader.readString();
				Integer val = reader.readInteger();
				//Integer val = jsonObj.getInteger(key);
						
				if(key.startsWith("NodeIndex:")) {
					String id = key.substring(10);
					indexNodeTable.put(val, id);
				}else if(key.startsWith("ActionIndex:")) {
					String id = key.substring(12);
					indexActionTable.put(val, id);
				}else if(key.startsWith("indexDateTable:")) {
					logger.info("$: "+key+", "+val);
					Date dt = Date.from(Instant.parse(key.substring(15)));
					indexDateTable.put(val,dt);
				}
			}
			reader.endArray();
			reader.close();
			//reader.endObject();
			//reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * directional
	 * @param jsonIndexFile
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public static void parseIndexJSOND(String jsonIndexFile, 
			Map<String,Integer> indexNodeTable,
			Map<String,Integer> indexActionTable, 
			Map<Date,Integer> indexDateTable) {
		
		try {
			//parse the json file
			//JSONObject jsonObj = JSONObject.parseObject(jsonIndexFile);
			
			JSONReader reader = new JSONReader(new FileReader(jsonIndexFile));
			reader.startArray();
			//reader.startObject();
			//while(String key:jsonObj.keySet()) {
			while(reader.hasNext()) {
				
				String key = reader.readString();
				Integer val = reader.readInteger();
				//Integer val = jsonObj.getInteger(key);
						
				if(key.startsWith("NodeIndex:")) {
					String id = key.substring(10);
					indexNodeTable.put(id,val);
				}else if(key.startsWith("ActionIndex:")) {
					String id = key.substring(12);
					indexActionTable.put(id,val);
				}else if(key.startsWith("indexDateTable:")) {
					logger.info("$: "+key+", "+val);
					Date dt = Date.from(Instant.parse(key.substring(15)));
					indexDateTable.put(dt,val);
				}
			}
			reader.endArray();
			reader.close();
			//reader.endObject();
			//reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * index map
	 * @param outputIndex
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public static void storeIndexesJSON(FileWriter outputIndex0, Map<String, Integer> indexNodeTable,
			Map<String, Integer> indexActionTable, Map<Date, Integer> indexDateTable) {
		// TODO Auto-generated method stub
		try {
			
			JSONWriter writer = new JSONWriter(outputIndex0);
			writer.startArray();	
			//outputIndex.append("NodeIndex: "+"\n");
			Iterator<Entry<String, Integer>> ier = indexNodeTable.entrySet().iterator();
			while(ier.hasNext()) {
				Entry<String, Integer> item = ier.next();
				writer.writeKey("NodeIndex:"+item.getKey());
				writer.writeValue(item.getValue());
				//outputIndex.append(item.getKey()+", "+item.getValue()+"\n");
			}
			
			//outputIndex.append("ActionIndex: "+"\n");
			Iterator<Entry<String, Integer>> ier2 = indexActionTable.entrySet().iterator();
			while(ier2.hasNext()) {
				Entry<String, Integer> item = ier2.next();
				writer.writeKey("ActionIndex:"+item.getKey());
				writer.writeValue(item.getValue());
				
				//outputIndex.append(item.getKey()+", "+item.getValue()+"\n");
			}
			//outputIndex.append("indexDateTable: "+"\n");
			Iterator<Entry<Date, Integer>> ier3 = indexDateTable.entrySet().iterator();
			while(ier3.hasNext()) {
				Entry<Date, Integer> item = ier3.next();
				writer.writeKey("indexDateTable:"+item.getKey().toInstant().toString());
				writer.writeValue(item.getValue());
				//outputIndex.append(item.getKey().toString()+", "+item.getValue()+"\n");
			}
			//outputIndex.flush();
			 writer.endArray();
			 writer.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private void storeIndexes0(FileWriter outputIndex, Map<String, Integer> indexNodeTable,
			Map<String, Integer> indexActionTable, Map<Date, Integer> indexDateTable) {
		// TODO Auto-generated method stub
		try {
			
			
			outputIndex.append("NodeIndex: "+"\n");
			Iterator<Entry<String, Integer>> ier = indexNodeTable.entrySet().iterator();
			while(ier.hasNext()) {
				Entry<String, Integer> item = ier.next();
				outputIndex.append(item.getKey()+", "+item.getValue()+"\n");
			}
			
			outputIndex.append("ActionIndex: "+"\n");
			Iterator<Entry<String, Integer>> ier2 = indexActionTable.entrySet().iterator();
			while(ier2.hasNext()) {
				Entry<String, Integer> item = ier2.next();
				outputIndex.append(item.getKey()+", "+item.getValue()+"\n");
			}
			outputIndex.append("indexDateTable: "+"\n");
			Iterator<Entry<Date, Integer>> ier3 = indexDateTable.entrySet().iterator();
			while(ier3.hasNext()) {
				Entry<Date, Integer> item = ier3.next();
				outputIndex.append(item.getKey().toString()+", "+item.getValue()+"\n");
			}
			outputIndex.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * month difference between two date
	 * @param date1
	 * @param date2
	 * @return
	 */
	private int getMonthIndex0(Date date1, Date date2) {
		// TODO Auto-generated method stub
		//logger.info(date1+", "+date2);
		   YearMonth m1 = YearMonth.of(date1.getYear(), date1.getMonth()+1);
		    YearMonth m2 = YearMonth.of(date2.getYear(), date2.getMonth()+1);

		    //m1 small
		    if(m1.isBefore(m2)){
		    	 return (int) (m1.until(m2, ChronoUnit.MONTHS) + 1);
		    }else if(m1.isAfter(m2)){
		    	 return (int) (m2.until(m1, ChronoUnit.MONTHS) + 1);
		    }else{
		    	return 0;
		    }
		    
		   
		 
	}

	private long getHourIndex(Date date1, Date date2) {
		// TODO Auto-generated method stub
		//logger.info(date1+", "+date2);
			
			boolean FirstBefore=date1.toInstant().isBefore(date2.toInstant());
			long millisecs=Math.abs(date1.toInstant().toEpochMilli()-
					date2.toInstant().toEpochMilli());
		
			long hour = Math.abs(millisecs/(1000*60*60));
			
		   //YearMonth m1 = YearMonthDay.of(date1.getYear(), date1.getMonth()+1);
		   // YearMonth m2 = YearMonth.of(date2.getYear(), date2.getMonth()+1);
		  
		    //m1 small
			return hour;
	}
	
	
private long getDayIndex0(Date date1, Date date2) {
	// TODO Auto-generated method stub
	//logger.info(date1+", "+date2);
		
		boolean FirstBefore=date1.toInstant().isBefore(date2.toInstant());
		long millisecs=Math.abs(date1.toInstant().toEpochMilli()-
				date2.toInstant().toEpochMilli());
	
		long days = Math.abs(millisecs/(1000*60*60*24));
		
	   //YearMonth m1 = YearMonthDay.of(date1.getYear(), date1.getMonth()+1);
	   // YearMonth m2 = YearMonth.of(date2.getYear(), date2.getMonth()+1);
	  
	    //m1 small
	    if(FirstBefore){
	    	
	    	 return days;//(int) (m1.until(m2, ChronoUnit.MONTHS) + 1);
	    }else if(!FirstBefore){
	    	
	    	 return days;//(int) (m2.until(m1, ChronoUnit.MONTHS) + 1);
	    }else{
	    	return 0;
	    }
	    
	   
	 
}
}


