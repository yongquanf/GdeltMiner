package EventCorrelationMining;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JFrame;

import org.apache.commons.compress.utils.Lists;
import org.math.plot.FrameView;
import org.math.plot.Plot2DPanel;
import org.math.plot.PlotPanel;
import org.math.plot.plots.BarPlot;
import org.math.plot.plots.ScatterPlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONReader;
import com.alibaba.fastjson.JSONWriter;
import com.google.common.collect.Maps;

import java.util.Set; 

import ETLPipeline.EventProcessStep;
import EventGraph.graphStream.kcore.Pair;
import util.Util.CosineSimilarity;

public class TopKAttribeQuery {

	private static final Logger logger = LoggerFactory.getLogger(TopKAttribeQuery.class);
	
	
	/**
	 * retrieve the top-k record
	 * @param outPutFile
	 */
	public void retrieveTopKFromFile(String outPutFile) {
		
		Map<String, Float> rec = retrieveFromJSON(outPutFile);
	}
	
	/**
	 * infer row siZe
	 * @param factorFileDir
	 * @param EventItemIndexFile
	 * @param outPutFile
	 * @param dim
	 * @param rank
	 * @param topKNum
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 * @throws Exception
	 */
	public void topKAnalysis(String factorFileDir,String EventItemIndexFile,
			String outPutFile,int dim,int rank,int topKNum,
			Map<Integer,String> indexNodeTable,
			Map<Integer,String> indexActionTable,
			Map<Integer,Date> indexDateTable) throws Exception{
		//get row size from file
		int[] rowSizes = EventProcessStep.getRowSizeFromFile(factorFileDir,dim);
		//perform analysis
		topKAnalysis(factorFileDir, EventItemIndexFile, outPutFile, 
				dim, rank, rowSizes, topKNum, 
				indexNodeTable, indexActionTable, indexDateTable);
	}
	 
	/**
	 * compute the pairwise similarity between events
	 * @throws Exception 
	 */
	public void topKAnalysis(String factorFileDir,String EventItemIndexFile,
			String outPutFile,int dim,int rank,int[] rowSizes,int topKNum,
			Map<Integer,String> indexNodeTable,
			Map<Integer,String> indexActionTable,
			Map<Integer,Date> indexDateTable) throws Exception{
		//step 1, load event item index;
		String delim = ",";
		BufferedReader br = new BufferedReader(new FileReader(EventItemIndexFile));
		
		//event map
		Map<Integer,List<Integer>> EventItemIndexMap = new 
				HashMap<Integer,List<Integer>>();
		while(true){
			String line = br.readLine();
			if(line==null)
				break;		
			//process step
			List<Integer> items = EventProcessStep.extractEventItemList(line,delim);
		
			EventItemIndexMap.put(items.get(0), items.subList(1, items.size()));
		}
		br.close();
		
		//step 2, load factor matrix
		//[n],[I_i],[K]

		//load the factor files
		float[][][] params= EventProcessStep.loadParamMatrices(factorFileDir,rowSizes,dim,rank);
		
		for(int topicIndex=0;topicIndex<rank;topicIndex++) {
		
			String topic = "TopicSort".concat(Integer.toString(topicIndex+1));
			
		//actor1
		List<Map.Entry<Integer,Float>> topactor1Values = retrievetopValues(topicIndex,params,
				dim,rank,1,topKNum);
		//writeTopK2File(outPutFile,topactor1Values,topKNum+"topactor1Values");
		storeIndexesJSON(outPutFile.concat(topic),indexNodeTable,indexActionTable,
				indexDateTable,topKIndexer.indexNodeTable,topactor1Values);
		
		
		
		
		//double[][] Y = parseTopKValues(rec);
		// displayResult(Y);
		
		//actor2
		List<Map.Entry<Integer,Float>> topactor2Values = retrievetopValues(topicIndex,params,
				dim,rank,2,topKNum);
		//writeTopK2File(outPutFile,topactor2Values,topKNum+"topactor2Values");
		storeIndexesJSON(outPutFile.concat(topic),indexNodeTable,indexActionTable,
				indexDateTable,topKIndexer.indexNodeTable,topactor2Values);
		
		
		//double[][] Y2 = parseTopKValues(topactor2Values);
		// displayResult(Y2);
		 
		//action
		List<Map.Entry<Integer,Float>> topActionValues = retrievetopValues(topicIndex,params,
				dim,rank,3,topKNum);
		//writeTopK2File(outPutFile,topActionValues,topKNum+"topActionValues");
		storeIndexesJSON(outPutFile.concat(topic),indexNodeTable,indexActionTable,
				indexDateTable,topKIndexer.indexActiTable,topActionValues);
		
		//double[][] Y3 = parseTopKValues(topActionValues);
		// displayResult(Y3);
		 
		//time
		List<Map.Entry<Integer,Float>> topTimeValues = retrievetopValues(topicIndex,params,
				dim,rank,4,topKNum);
		//writeTopK2File(outPutFile,topTimeValues,topKNum+"topTimeValues");	
		storeIndexesJSON(outPutFile.concat(topic),indexNodeTable,indexActionTable,
				indexDateTable,topKIndexer.indexDateTable,topTimeValues);
		
		//double[][] Y4 = parseTopKValues(topTimeValues);
		// displayResult(Y4);
		 
		//step 4, Return the list of similarity from the selected file to the other files
		}
		
		
		EventItemIndexMap.clear();
		params = null;
	}
	

	/**
	 * retrieve the 2-d metrics
	 * col-1: integer; col-2: metric
	 * @param topactor1Values
	 * @return
	 */
	private double[][] parseTopKValues(List<Entry<Integer, Float>> topactor1Values) {
		// TODO Auto-generated method stub
		double[][] out = new double[topactor1Values.size()][2];
		for(int i=0;i<topactor1Values.size();i++) {			
				out[i][0] = i+1;//topactor1Values.get(i).getKey();
				out[i][1] = topactor1Values.get(i).getValue();
			
		}
		return out;
	}


	static void displayResult(double[][] Y) {
		Plot2DPanel plot = new Plot2DPanel();

		BarPlot dataPlot = new BarPlot("Data", PlotPanel.COLORLIST[0], Y);
		plot.plotCanvas.setNotable(true);
		plot.plotCanvas.setNoteCoords(true);
		plot.plotCanvas.addPlot(dataPlot);

		FrameView plotframe = new FrameView(plot);
		plotframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		plotframe.setVisible(true);
	}
	
	
	/**
	 * write to file
	 * @param outPutFile
	 * @param list
	 * @param header
	 */
	public void writeTopK2File(String outPutFile, List<Map.Entry<Integer,Float>> list, String header) {
		
		try {
			//append
			BufferedWriter outSimilarity = new BufferedWriter(new FileWriter(outPutFile,true));
			outSimilarity.append(header+"\n");
			for(Entry<Integer, Float> item:list) {
				outSimilarity.append(item.getKey()+" "+item.getValue()+"\n");
			}
			outSimilarity.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * parse
	 * @param jsonIndexFile
	 * @param topKResult
	 * @param header
	 */
	public static void parseIndexJSON(String jsonIndexFile, Map<String,Float> topKResult,String header) {
		
		try {
			JSONReader reader = new JSONReader(new FileReader(jsonIndexFile));
			reader.startObject();
			while(reader.hasNext()) {
				String key = reader.readString();
				float val = reader.readObject(Float.class);
				if(key.startsWith(header)) {
					int pos = key.indexOf(header)+header.length();
					String id = key.substring(pos);
					topKResult.put(id,val);
				}
			}
			
			reader.endObject();
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * index
	 * @author eric
	 *
	 */
	public enum topKIndexer{
		indexNodeTable,
		indexActiTable,
		indexDateTable};
	
	/**
	 *  1 Map<Integer,String> indexNodeTable = Maps.newHashMap();
		2 Map<Integer,String> indexActionTable = Maps.newHashMap();
		3  Map<Integer,Date> indexDateTable = Maps.newHashMap();
	 * @param outPutFile
	 * @param list
	 * @param choicer
	 */
	public static void storeIndexesJSON(String outPutFile,
			Map<Integer,String> indexNodeTable,
			Map<Integer,String> indexActionTable,
			Map<Integer,Date> indexDateTable,
			topKIndexer choicer,
			List<Map.Entry<Integer,Float>> list) {
		// TODO Auto-generated method stub
		try {
			//file
			
			FileWriter xx = new FileWriter(new File(outPutFile),true);
			
			JSONWriter writer = new JSONWriter(xx);
			writer.startObject();
			
			
			//outputIndex.append("NodeIndex: "+"\n");
			Iterator<Entry<Integer, Float>> ier = list.iterator();
			while(ier.hasNext()) {
				Entry<Integer, Float> item = ier.next();
				//index of the entity
				int index=item.getKey();
				String id="";
				switch(choicer) {
				case indexNodeTable:{
					id = indexNodeTable.get(index);
					break;
				}
				case indexActiTable:{
					id = indexActionTable.get(index);
					break;
				}
				case indexDateTable:{
					
					id = indexDateTable.get(index).toInstant().toString();// indexDateTable.get(index).toInstant().toString();
					break;
				}
				}
				
				writer.writeKey(choicer+"_"+id);
				writer.writeValue(item.getValue());
				//outputIndex.append(item.getKey()+", "+item.getValue()+"\n");
			}
			writer.endObject();
			writer.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * retrieve from json
	 * @param outPutFile
	 * @return
	 */
	public static Map<String,Float> retrieveFromJSON(String outPutFile) {

		try {
			//parse the json file
			//JSONObject jsonObj = JSONObject.parseObject(jsonIndexFile);
			Map<String,Float> list = Maps.newHashMap();
			
			JSONReader reader = new JSONReader(new FileReader(outPutFile));
			reader.startObject();
			//reader.startObject();
			//while(String key:jsonObj.keySet()) {
			while(reader.hasNext()) {
				
				String key = reader.readString();
				
				float val = reader.readObject(Float.class);
				
				logger.info("Retrieve: "+key+", "+val);
				
				//Integer val = jsonObj.getInteger(key);
				list.put(key, val);
				 
				
			}
			reader.endObject();
			//reader.close();
			
			return list;
			//reader.endObject();
			//reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
		
	}
	
	
	/**
	 * retrieve topK values
	 * @param params
	 * @param dim
	 * @param rank
	 * @param indexDim
	 * @param topKNum
	 * @return
	 */
	public List<Map.Entry<Integer,Float>> retrievetopValues(int topicDimension,
			float[][][] params, int dim, int rank, int indexDim, int topKNum) {
		// TODO Auto-generated method stub
		
		/**
		 * comparator
		 */
		Comparator<Map.Entry<Integer,Float>> byMapValue = new Comparator<Map.Entry<Integer,Float>>(){

			@Override
			public int compare(Entry<Integer, Float> left, Entry<Integer, Float> right) {
				// TODO Auto-generated method stub
				return left.getValue().compareTo(right.getValue());
			}
		};
		
		//selector of dimension
		float[][] items = params[indexDim-1];
		//index,norm
		List<Map.Entry<Integer,Float>> itemValues = GetNorm(topicDimension,items);
		Collections.sort(itemValues, byMapValue);
		Collections.reverse(itemValues);
		
		return itemValues.subList(0,Math.min(items.length,topKNum));
	}
	
	/**
	 * norm
	 * @param topicDimension current topic dimension
	 * @param items, factor matrix
	 * @return
	 */
	private List<Map.Entry<Integer,Float>> GetNorm(int topicDimension, float[][] items) {
		// TODO Auto-generated method stub
		
		List<Map.Entry<Integer,Float>> ls = new ArrayList<Map.Entry<Integer,Float>>();
		
		Map<Integer,Float> rec = new HashMap<Integer,Float>();
		
		int len = items.length;
		for(int i=0;i<len;i++) {
			 float[] vec = items[i];
			float sum = 0;
			  sum = (float) Math.pow(vec[topicDimension], 2);
			 if(false) {
				 for(int j=0;j<vec.length;j++) {
					 sum+=Math.pow(vec[j], 2);
				 }
			 }
			 sum = (float) Math.sqrt(sum);
			 rec.put(i,sum);
		}
		ls.addAll(rec.entrySet());
		return ls;
	}

	/**
	 * map to the raw data
	 * @param factorFileDir
	 * @param EventItemIndexFile
	 * @param outPutFile
	 * @param dim
	 * @param rank
	 * @param rowSizes
	 * @param topKNum
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 * @throws Exception
	 */
	public void topKAnalysis0(String factorFileDir,String EventItemIndexFile,
			String outPutFile,int dim,int rank,int[] rowSizes,int topKNum) throws Exception {
		// TODO Auto-generated method stub
		//step 1, load event item index;
				String delim = ",";
				BufferedReader br = new BufferedReader(new FileReader(EventItemIndexFile));
				
				//event map
				Map<Integer,List<Integer>> EventItemIndexMap = new HashMap<Integer,List<Integer>>();
				while(true){
					String line = br.readLine();
					if(line==null)
						break;		
					//process step
					List<Integer> items = EventProcessStep.extractEventItemList(line,delim);
				
					EventItemIndexMap.put(items.get(0), items.subList(1, items.size()));
				}
				br.close();
				
				//step 2, load factor matrix
				//[n],[I_i],[K]

				//load the factor files
				float[][][] params= EventProcessStep.loadParamMatrices(factorFileDir,rowSizes,dim,rank);
					
				
				for(int topicIndex=0;topicIndex<rank;topicIndex++) {
					
					String topic = "TopicSort".concat(Integer.toString(topicIndex+1));
				
				//actor1
				List<Map.Entry<Integer,Float>> topactor1Values = retrievetopValues(topicIndex, params,
						dim,rank,1,topKNum);
				writeTopK2File(outPutFile.concat(topic),topactor1Values,topKNum+"topactor1Values");
				
				double[][] Y = parseTopKValues(topactor1Values);
				 displayResult(Y);
				
				//actor2
				List<Map.Entry<Integer,Float>> topactor2Values = retrievetopValues(topicIndex,params,
						dim,rank,2,topKNum);
				writeTopK2File(outPutFile.concat(topic),topactor2Values,topKNum+"topactor2Values");
				
				double[][] Y2 = parseTopKValues(topactor2Values);
				 displayResult(Y2);
				 
				//action
				List<Map.Entry<Integer,Float>> topActionValues = retrievetopValues(topicIndex,params,dim,
						rank,3,topKNum);
				writeTopK2File(outPutFile.concat(topic),topActionValues,topKNum+"-topActionValues");
				
				double[][] Y3 = parseTopKValues(topActionValues);
				 displayResult(Y3);
				 
				//time
				List<Map.Entry<Integer,Float>> topTimeValues = retrievetopValues(topicIndex,params,dim,
						rank,4,topKNum);
				writeTopK2File(outPutFile.concat(topic),topTimeValues,topKNum+"-topTimeValues");	
				
				double[][] Y4 = parseTopKValues(topTimeValues);
				 displayResult(Y4);
				 
				//step 4, Return the list of similarity from the selected file to the other files
				}
				
				
				EventItemIndexMap.clear();
				params = null;
	}
	
}
