package ETLPipeline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.nd4j.linalg.io.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Doubles;

import util.Cluster.MultDimCluster;
import util.Cluster.MultDimData;
import util.Cluster.clusterChoice;
import util.Util.CosineSimilarity;
import util.associationMining.AnalysisResult;
import util.associationMining.AprioriAlgorithm;
import util.associationMining.AprioriTimeoutException;
import util.associationMining.Transaction;
import util.csv.GDELTReturnResult;
import util.models.GdeltEventResource;
import util.tensor.sals.single.SALS;

public class EventProcessStep {

	private static final Logger logger = LoggerFactory.getLogger(EventProcessStep.class);
	/**
	 * association rule mining
	 */
	AprioriAlgorithm apriori;
	
	public EventProcessStep(){
		
	}
	
	/**
	 * step 2. frequency items mining
	 * @param rawEventLists, input
	 * @param minSupport, frequent items
	 * @param minConfidence, association confidence
	 */
	public AnalysisResult frequentItemsSearch(GDELTReturnResult rawEventLists,Double minSupport, Double minConfidence){
		
		apriori = new AprioriAlgorithm(minSupport, minConfidence);
		AnalysisResult result = null;
		try {
			result = apriori.analyze(getTransactions(rawEventLists));
		} catch (AprioriTimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * step 3, cluster by one dimension
	 * events: {actor1,actor2,action,eventToneGoldstein,eventMensions}
	 * @param rawEventLists
	 * @param dim 
	 * @param kmeansPPClusterNum, 10 by default
	 * @param maxIters, 100 by default,
	 * @return 
	 */
	public List<CentroidCluster<MultDimData>> clusterBySingleDimension(GDELTReturnResult rawEventLists,clusterChoice choice,int kmeansPPClusterNum,int maxIters){
		List<MultDimData> clusterInput = extractMultDimInput(rawEventLists,choice);
		System.out.print(clusterInput.size()+", "+clusterInput.get(0).getPoint().length);
		MultDimCluster cluster = new MultDimCluster();
		return cluster.clusterKMeansPP(clusterInput, kmeansPPClusterNum, maxIters);		
	}
	
	
	/**
	 * step 3, calculate tensor factor by sals
	 * @param rawEventLists
	 */
	public void tensorFactor(String[] args, boolean useBias){

		try {
			SALS.run(args, useBias);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * compute the pairwise similarity between events
	 * @throws Exception 
	 */
	public void similarityAnalysis(String factorFileDir,
			String EventItemIndexFile,String outPutFile,
			int dim,int rank,int[] rowSizes) throws Exception{
		//step 1, load event item index;
		String delim = ",";
		BufferedReader br = new BufferedReader(new FileReader(EventItemIndexFile));
		
		//event map
		Map<Integer,List<Integer>> EventItemIndexMap = new HashMap<Integer,List<Integer>>();
		while(true){
			String line = br.readLine();
			if(line==null)
				break;		
			List<Integer> items = extractEventItemList(line,delim);
			EventItemIndexMap.put(items.get(0), items.subList(1, items.size()));
		}
		br.close();
		
		//step 2, load factor matrix
		//[n],[I_i],[K]
		BufferedWriter outSimilarity = new BufferedWriter(new FileWriter(outPutFile));
		//load the factor files
		float[][][] params=loadParamMatrices(factorFileDir,rowSizes,dim,rank);
				
		
		Set<Integer> allKeys = EventItemIndexMap.keySet();
		Iterator<Integer> ier1 = allKeys.iterator();
		while(ier1.hasNext()) {
			
			int eventGlobalID = ier1.next();
			
		//step 3, for each pair of event, compute the similarity
		Iterator<Integer> ier = EventItemIndexMap.keySet().iterator();
		while(ier.hasNext()) {
			Integer yourEventGlobalID = ier.next();
			int[] yourIndex = getRowIndex( yourEventGlobalID,EventItemIndexMap);
			int[] myIndex = getRowIndex(eventGlobalID,EventItemIndexMap); 
			double[] simVal =CosineSimilarity.computeByDim(params,myIndex,yourIndex);
			
			outSimilarity.append(eventGlobalID+","+yourEventGlobalID+stringRepresent(simVal,",")+"\n");
		}
		
		}//end
		outSimilarity.flush();
		outSimilarity.close();
		
		//step 4, Return the list of similarity from the selected file to the other files
		
		EventItemIndexMap.clear();
		params = null;
	}
	
	/**
	 * 
	 * @param simVal
	 * @param decimit
	 * @return
	 */
	public static String stringRepresent(double[] simVal, String decimit) {
		// TODO Auto-generated method stub
		StringBuilder sbuilder = new StringBuilder();
		for(int i=0;i<simVal.length;i++) {
			sbuilder.append(decimit);
			sbuilder.append(simVal[i]);
			
		}
		return sbuilder.toString();
	}

	/**
	 * row indexes
	 * @param yourEventGlobalID
	 * @param eventItemIndexMap
	 * @return
	 */
	public static int[] getRowIndex(Integer yourEventGlobalID, Map<Integer, List<Integer>> eventItemIndexMap) {
		// TODO Auto-generated method stub
		List<Integer> indexes = eventItemIndexMap.get(yourEventGlobalID);
		
		 int[] index = new int[indexes.size()];		
		 for( int i=0;i<indexes.size();i++) {
			 index[i] = indexes.get(i);
		 }
		 return index;
	}

	/**
	 * first column of each file
	 * @param factorFileDir
	 * @param dim
	 * @return
	 * @throws Exception 
	 */
	public static int[] getRowSizeFromFile(String factorFileDir,int dim) throws Exception {
		String outputDir = factorFileDir+File.separator+"factor_matrices";
		int[] counterPerDim = new int[dim];
		for(int i=0;i<dim;i++) {
			BufferedReader br = new BufferedReader(new 
					FileReader(outputDir+File.separator+(i+1)));
			int counter=0;
			while(true){
				String line = br.readLine();
				if(line==null)
					break;
				//
				String[]x=line.split(",");
				counter=Integer.valueOf(x[0]);
				
			}
			br.close();
			counterPerDim[i]= counter+1;
		}
		return counterPerDim;
	}
	/**
	 * 
	 * @param factorFileDir
	 * @param dim
	 * @param rank
	 * @return
	 */
	public static float[][][] loadParamMatrices(String factorFileDir, 
			int[] countPerDim,int dim, int rank) {
		// TODO Auto-generated method stub
		String outputDir = factorFileDir+File.separator+"factor_matrices";
		float[][][] params = new float[dim][][];
		//set the dimension for each factor matrix
		for(int i=0;i<dim;i++) {
			params[i]=new float[countPerDim[i]][rank];
		}
		
		for(int n=0; n<dim; n++){
			try {
				BufferedReader br = new BufferedReader(new 
						FileReader(outputDir+File.separator+(n+1)));
				//iterate
				while(true){
				String line = br.readLine();
				if(line==null)
					break;
				//
				String[]x=line.split(",");
				int RowIndex=Integer.valueOf(x[0]);
				for(int j=0;j<rank;j++) {
					params[n][RowIndex][j]=Float.valueOf(x[j+1]);
				}
				}
				br.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}//end
		return params;
	}

	public static List<Integer> extractEventItemList(String line,String delim) {
		// TODO Auto-generated method stub
		List<Integer> list = new ArrayList<Integer>();
		String[] tokens = line.split(delim);
		for(int i=0;i<tokens.length;i++) {
			list.add(Integer.valueOf(tokens[i]));			
		}
		return list;
	}

	/**
	 * extract events
	 * {actor1,actor2,action,eventToneGoldstein,eventMensions}
	 * @param rawEventLists
	 * @param dim
	 * @return
	 */
	private List<MultDimData> extractMultDimInput(GDELTReturnResult rawEventLists, clusterChoice choice) {
		// TODO Auto-generated method stub
		
		List<MultDimData> list = new ArrayList<MultDimData>();
		
		Iterator<GdeltEventResource> ier = rawEventLists.getGdeltEventList().iterator();
		while(ier.hasNext()){
			GdeltEventResource event = ier.next();
			switch(choice){
			case actor1:{
				double L1 = event.getActor1GeoLat();
				double L2 = event.getActor1GeoLong();
				if(Double.isNaN(L1)||Double.isNaN(L2)||Double.isInfinite(L1)||Double.isInfinite(L2)){
					continue;
				}
				double[] _point=new double[2];
				_point[0] = L1;
				_point[1] = L2;
				//System.out.println(L1+" "+L2);
				MultDimData one = new MultDimData(_point);
				list.add(one);
				break;
			}//
			case actor2:{
				double L1 = event.getActor2GeoLat();
				double L2 = event.getActor2GeoLong();
				if(Double.isNaN(L1)||Double.isNaN(L2)||Double.isInfinite(L1)||Double.isInfinite(L2)){
					continue;
				}
				double[] _point=new double[2];
				_point[0] = L1;
				_point[1] = L2;
				//System.out.println(L1+" "+L2);
				MultDimData one = new MultDimData(_point);
				list.add(one);
				break;
			}
			case action:{
				double L1 = event.getActionGeoLat();
				double L2 = event.getActionGeoLong();
				if(Double.isNaN(L1)||Double.isNaN(L2)||Double.isInfinite(L1)||Double.isInfinite(L2)){
					continue;
				}
				double[] _point=new double[2];
				_point[0] = L1;
				_point[1] = L2;
				//System.out.println(L1+" "+L2);
				MultDimData one = new MultDimData(_point);
				list.add(one);
				break;
			}
			case eventToneGoldstein:{
				double L1 = event.getAvgTone()/util.models.GdeltConstants.AvgToneScale;
				double L2 = event.getGoldsteinScale()/util.models.GdeltConstants.GoldsteinScale;
				if(Double.isNaN(L1)||Double.isNaN(L2)||Double.isInfinite(L1)||Double.isInfinite(L2)){
					continue;
				}
				double[] _point=new double[2];
				_point[0] = L1;
				_point[1] = L2;
				//System.out.println(L1+" "+L2);
				MultDimData one = new MultDimData(_point);
				list.add(one);
				break;
			}
			case eventMensions:{
				double L1 = event.getNumMentions();
				double L2 = event.getNumSources();
				double L3 = event.getNumArticles();
				if(Double.isNaN(L1)||Double.isNaN(L2)||Double.isNaN(L3)||Double.isInfinite(L1)||Double.isInfinite(L2)||Double.isInfinite(L3)){
					continue;
				}
				double[] _point=new double[3];
				_point[0] = L1;
				_point[1] = L2;
				_point[2] = L3;
				//System.out.println(L1+" "+L2);
				MultDimData one = new MultDimData(_point);
				list.add(one);
				break;
			}
			}
		
		}
		return list;
			
	}

	/**
	 * retrieve transaction from the events
	 * 
	 * @return
	 */
	private List<Transaction> getTransactions(GDELTReturnResult rawEventLists){
		List<Transaction> transactions = new ArrayList<Transaction>();
		
		Iterator<GdeltEventResource> ier = rawEventLists.getGdeltEventList().iterator();
		while(ier.hasNext()){
			GdeltEventResource event = ier.next();
			Transaction trasOne = ExtractTransaction(event);
			//skip
			if(trasOne==null){continue;}
			transactions.add(trasOne);
		}
		return transactions;
	}
	
	/**
	 * extract transactions
	 * @param event
	 * @return
	 */
	private Transaction ExtractTransaction(GdeltEventResource event) {
		// TODO Auto-generated method stub
		Set<String> items = new HashSet<String>();
		String from = event.getActor1CountryCode();
		String to = event.getActor2CountryCode();
		String code = event.getEventBaseCode();
		//skip
		if(from.isEmpty()||to.isEmpty()||code.isEmpty()){
			return null;
		}
		//actor1
		items.add(from);
		//actor2
		items.add(to);
		//eventbase
		items.add(code);
		
		System.out.println(from+"\t"+to+"\t"+code);
		
        return new Transaction(items);
	}
	
}
