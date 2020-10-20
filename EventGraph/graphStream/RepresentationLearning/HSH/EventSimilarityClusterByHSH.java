package EventGraph.graphStream.RepresentationLearning.HSH;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.graphstream.algorithm.util.RandomTools;
import org.nd4j.linalg.io.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ETLPipeline.EventProcessStep;
import ETLPipeline.DirParser.ETLDirApp;
import util.Cluster.MultDimCluster;
import util.Cluster.MultDimData;
import util.Util.CosineSimilarity;
import util.Util.smile.math.Math;
import util.Util.smile.math.matrix.DenseMatrix;
import util.Util.smile.math.matrix.Matrix;

/**
 * we fix H, solve for S
 * for each vertex, we compute a relative coordinate for each node,
 * relative coordinate: distance to a number of landmarks
 * then, we solve for S only
 * @author eric
 *
 */
public class EventSimilarityClusterByHSH {

	
	private static final Logger logger = LoggerFactory.getLogger(EventSimilarityClusterByHSH.class);
	
	/**
	 * number of landmarks
	 */
	int landmarkNum = 24;
	/**
	 * event-item index
	 */
	Map<Integer,List<Integer>> EventItemIndexMap;
	/**
	 * latent factor
	 */
	float[][][] params;
	/**
	 * index of landmark events
	 */
	List<Integer> landmarks;
	/**
	 * index of all events
	 */
	List<Integer> allKeys;
	
	/**
	 * constructor
	 */
	public EventSimilarityClusterByHSH(int _landmarkNum){
	
		EventItemIndexMap = 
				new HashMap<Integer,List<Integer>>();
		
		landmarkNum = _landmarkNum;
	}
	
	public EventSimilarityClusterByHSH(){		
		EventItemIndexMap = 
				new HashMap<Integer,List<Integer>>();

	}
	/**
	 * relative coordinate based clustering
	 * @param args
	 */
	public static void main(String[] args) {
		printError(); 
		EventSimilarityClusterByHSH test = new EventSimilarityClusterByHSH();		
		test.clusterByRelCoordinate(args); 
	}
	
	 private static void printError() {
	        System.err.println("java EventSimilarityClusterByHSH kmeansPPClusterNumLower kmeansPPClusterNumUpper maxIters landmarkNum..."
	        		+ "outFileName EventItemIndexFile factorFileDir dim rank I1,I2,I3,I4...  ");
	        
	 }
	
	/**
	 * portal
	 * @param args
	 * kmeansPPClusterNumLower kmeansPPClusterNumUpper maxIters landmarkNum\... 
	 *  outFileName EventItemIndexFile factorFileDir dim rank I1,I2,I3,I4...  
	 */
	public void clusterByRelCoordinate(String[] args) {
		
		/**
		 * k-means ,low bound to upper bound
		 */
		int kmeansPPClusterNumLower= Integer.parseInt(args[0]);
		int kmeansPPClusterNumUpper= Integer.parseInt(args[1]);
		 int maxIters = Integer.parseInt(args[2]);
		 landmarkNum = Integer.parseInt(args[3]);
		/**
		 * output cluster result
		 */
		String outFileName = args[4];
		/**
		 * event-item reference file
		 */
		String EventItemIndexFile = args[5];
		/**
		 * tensor
		 */
		int start = 6;
		 String factorFileDir = args[start];
		 /**
		  * dimension
		  */
		int dim = Integer.valueOf(args[start +1]);
		/**
		 * rank
		 */
		int rank = Integer.valueOf(args[start + 2]);
		 
		/**
		 * size of each dimension
		 */
		 //I1,I2,I3,I4
		 int[] rowSizes = new int[Integer.valueOf(dim)];
		 for(int i=0;i<rowSizes.length;i++){
			 rowSizes[i] = Integer.valueOf(args[start+3+i]);
		 }
		 /**
		  * parse
		  */
		 try {
			factorCorrelateInterface(factorFileDir, dim, rank,
						rowSizes,	EventItemIndexFile);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		 
		 /**
		  * build coord
		  */
		 DenseMatrix relCoord=constructRelativeCoordinate();
		 logger.info("rel coord: "+relCoord.nrows()+"," +relCoord.ncols());
		 /**
		  * cluster
		  */
		// int maxIters = 500;
		 
		 FileWriter output;
			try {
		output = new FileWriter(new File(outFileName), true);
		//write the index of each coordinate
		for(int i=0;i<relCoord.nrows();i++) {
			output.append(allKeys.get(i)+": "+
					Arrays.toString(relCoord.getRowVector(i, relCoord.ncols()))+"\n");
		}
		output.flush();
		
		 for(int kmeansPPClusterNum=kmeansPPClusterNumLower;kmeansPPClusterNum<kmeansPPClusterNumUpper;kmeansPPClusterNum++) {
		 
		 List<CentroidCluster<MultDimData>> cResult= KMeansClusterRelCoordinate(relCoord,
					kmeansPPClusterNum, maxIters);
		 
		
			output.append("#clusterParams: "+kmeansPPClusterNum+", "+maxIters+"\n");
			//write result
			ETLDirApp.writeClusterResult(output, null, cResult);
			
		}
			
		 output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		 
	}
	
	
	
	/**
	 * extract the interface
	 * arg-tensor
	 * event-itemset index
	 * @throws Exception 
	 */
	public void factorCorrelateInterface(String factorFileDir, int dim, int rank,
			int[] rowSizes,	String EventItemIndexFile) throws Exception {
		int start = -1;
		
		 
		//step 1, load event item index;
			String delim = ",";
			BufferedReader br = new BufferedReader(new FileReader(EventItemIndexFile));
			
			//event map
			
			while(true){
				String line = br.readLine();
				if(line==null)
					break;		
				List<Integer> items = EventProcessStep.extractEventItemList(line,delim);
				EventItemIndexMap.put(items.get(0), items.subList(1, items.size()));
			}
			br.close();
			
			logger.info("parsed: "+EventItemIndexMap.size());
			//step 2, load factor matrix
			//[n],[I_i],[K]
			
			//load the factor files
			params= EventProcessStep.loadParamMatrices(factorFileDir,rowSizes,dim,rank);
			logger.info("factor: "+params.length+"," +params[0].length+", "+params[0][0].length);
			
			//add keys
			allKeys = Lists.newArrayList(); 
			allKeys.addAll(EventItemIndexMap.keySet());
			filterAllZeroItems(allKeys); 
			
			logger.info("keys: "+allKeys.size());
			/**
			 * select random sample as landmarks of the items
			 */
			Set<Integer> landamarkIndex = RandomTools.randomKsubset(allKeys.size(), 
					landmarkNum, null, new Random(System.currentTimeMillis()));
			//filterout all-0 entries
			landmarks = selectItemsByIndex(allKeys,landamarkIndex,params);
			
			logger.info("landmarks: "+landmarks.size());
			
			landamarkIndex.clear();
						
	}
	
	/**
	 * filter
	 * @param allKeys 
	 */
	private void filterAllZeroItems(List<Integer> allKeys) {
		int rows = allKeys.size();
	    Iterator<Integer> ier = allKeys.iterator();
	    while(ier.hasNext()) {
	    	
			int eventGlobalID = ier.next();
			
				int[] index1 = EventProcessStep.getRowIndex(eventGlobalID,EventItemIndexMap); 
				
				int actor1IndexEvent1 = index1[0];
				int actor2IndexEvent1 = index1[1];
				int action1IndexEvent1 = index1[2];
				int dateIndexEvent1 = index1[3];
				
				float[] actor1E1 = params[0][actor1IndexEvent1];
				float[] actor2E1 = params[1][actor2IndexEvent1];
				float[] actionE1 = params[2][action1IndexEvent1];
				float[] dateE1 =   params[3][dateIndexEvent1];
				//remove this item
				if(zeroTest(actor1E1)||zeroTest(actor2E1)||zeroTest(actionE1)||zeroTest(dateE1)) {
					ier.remove();
				}
			
			}//end all keys
		
	}
	
			private boolean zeroTest(float[] actor1e1) {
		// TODO Auto-generated method stub
				int countZero = 0;
				for(int i=0;i<actor1e1.length;i++) {
					if(Math.abs(actor1e1[i])<Math.pow(10, -15)) {
						countZero++;
					}
				}
				//half
				if(countZero>actor1e1.length/2.0) {
					return true;
				}else {
					return false;
				}
			}

			/**
			 * select samples.
			 * @param allKeys
			 * @param landamarkIndex
			 * @param params 
			 * @return
			 */
	private List<Integer> selectItemsByIndex(List<Integer> allKeys, 
			Set<Integer> landamarkIndex, float[][][] params) {
		// TODO Auto-generated method stub
		List<Integer> out = Lists.newArrayList();
		for(Integer idx: landamarkIndex) {
			out.add(allKeys.get(idx));
		}
		return out;
	}


	
	/**
	 * 2.compute the graph distance to a number of nodes
	 */
	public DenseMatrix constructRelativeCoordinate() {
		
		//create the relative-coordinate matrix
		int rows = allKeys.size();
		int cols = landmarks.size();
		DenseMatrix one = Matrix.newInstance(rows, cols, 0);
		
		for(int i=0;i<allKeys.size();i++) {
			int eventGlobalID = allKeys.get(i);
			for(int j=0;j<landmarks.size();j++) {
				Integer yourEventGlobalID = landmarks.get(j);
				
				int[] yourIndex = EventProcessStep.getRowIndex( yourEventGlobalID,EventItemIndexMap);
				
				int[] myIndex = EventProcessStep.getRowIndex(eventGlobalID,EventItemIndexMap); 
				
				double[] simVal =CosineSimilarity.computeByDim(params,myIndex,yourIndex);
				//set the value
				one.set(i, j, simVal[0]);
				}
			}//end all keys
		
		return one;
	}
	
	/**
	 * do K-means, optimal HSH
	 */
	public List<CentroidCluster<MultDimData>> KMeansClusterRelCoordinate(DenseMatrix relCoordMatrix,
			int kmeansPPClusterNum, int maxIters) {
		//todo hsh for clustering
		List<MultDimData> coords = MultDimCluster.extractRelCoordinate(relCoordMatrix);
		return MultDimCluster.clusterKMeansPP(coords, kmeansPPClusterNum, maxIters);	
	}
}
