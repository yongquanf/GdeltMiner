package ETLPipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Cluster.MultDimData;
import util.Cluster.clusterChoice;
import util.associationMining.AnalysisResult;
import util.csv.GDELTReturnResult;

public class ETLApp {

	private static final Logger logger = LoggerFactory.getLogger(ETLApp.class);
	
	public static  String fileTest="";
	
	EventProcessStep ep = new  EventProcessStep();
	EventExtractStep et = new EventExtractStep();
	
	
	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args){
		
		ETLApp test = new ETLApp();
		
		printError();

		
		//extract
		int selector = Integer.valueOf(args[0]);
		//extract tensor
		if(selector==1){
			String dataFile = args[1]; //date for parsing
			GDELTReturnResult result = test.EventETTest(dataFile);
			String format = test.EventETExtractTest(result);
			if(format!=null){
				logger.info("Tensor: "+format);
			}else{
				logger.error("tensor failed!");
			}
		//mining events	
		}else if(selector==2){
			int starter = 2;
			String dataFile = args[1]; //date for parsing
			GDELTReturnResult result = test.EventETTest(dataFile);
			//double minSupport, double minConfidence,int kmeansPPClusterNum
			double minSupport = Double.valueOf(args[starter]);
			double minConfidence = Double.valueOf(args[starter+1]);
			int kmeansPPClusterNum = Integer.valueOf(args[starter+2]);
			//event components
			test.EPTestAssociationAndCluster(minSupport, minConfidence, kmeansPPClusterNum,result);
		//mine the tensor
		}else if(selector==3){//create tensor factorization
			//[0 training  output M  N K [I1] [I2] ... [IN]]
			//event represent
			int startIndex = 5;
			test.EPTestTensorFactor(args,startIndex);
		}else if(selector ==4) {//create similarity
			int start = 5;
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
			 String EventItemIndexFile = training4TensorFactorization+"_EventItemsIndex";
			//out
			String outEventSimPutFile=factorFileDir+File.separator+"outEventSimPutFile";
			
			//event similarity analysis
			test.SimpleEventCorrelationAnalysis(factorFileDir, EventItemIndexFile, outEventSimPutFile,N, K, len);
		}
	}
	
	/**
	 * output of the etl pipeline
	 */
	 private static void printError() {
	        System.err.println("java ETLApp selector dataFile ,..."
	        		+ "minSupport minConfidence kmeansPPClusterNum "
	        		+ "training4TensorFactorization factorFileDir"  
	        		+ "M N K  {I1,I2,I3,I4,...}");
	        System.err.println("selector: "+"[1,2,3,4]"+"1: extract tensor"+", 2: Association mining And k-means Cluster "+", 3: calculate tensor factor by sals"+", 4: create event similarity file");
	        System.err.println("dataFile: "+ "GDELT trace input");
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
	 * extract
	 * @param dataFile 
	 * @return
	 */
	public GDELTReturnResult EventETTest(String dataFile){
		
		fileTest = dataFile;
		EventExtractStep et = new EventExtractStep();
		//read
		GDELTReturnResult gresult = et.readCSVTrace(fileTest);
		logger.info(gresult.toString()+"\n");
		logger.info(gresult.getDownloadResult().toString()+"\n");
		logger.info(gresult.getGdeltEventList().size()+"\n");
		//extract to tensor
		//et.Extract4dTensor(gresult, "fileTestTensor");
		return gresult;
	}
	
	/**
	 * extract the event
	 * @param gresult
	 */
	public String  EventETExtractTest(GDELTReturnResult gresult){
		
		
		//read
		//GDELTReturnResult gresult = et.readCSVTrace(fileTest);
		//logger.info(gresult.toString()+"\n");
		//logger.info(gresult.getDownloadResult().toString()+"\n");
		//logger.info(gresult.getGdeltEventList().size()+"\n");
		//extract to tensor
		return et.Extract4dTensorV0(gresult, "fileTestTensor");
		 
	}
	
	/**
	 * process the returned
	 * @param minSupport
	 * @param minConfidence
	 * @param kmeansPPClusterNum
	 * @param result
	 */
	public void EPTestAssociationAndCluster(double minSupport, double minConfidence,int kmeansPPClusterNum,GDELTReturnResult result){
		 
		FileWriter output;
		try {
			String outFileName = "EPTestAssociationAndCluster";
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
	public void SimpleEventCorrelationAnalysis(String factorFileDir, String EventItemIndexFile, String outEventSimPutFile, int dim, int rank, int[] rowSizes) {
		try {
			this.ep.similarityAnalysis(factorFileDir, EventItemIndexFile, outEventSimPutFile, dim, rank, rowSizes);
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
	private void writeClusterResult(FileWriter output, clusterChoice action, List<CentroidCluster<MultDimData>> cResult) throws IOException {
		// TODO Auto-generated method stub
		
		output.write("choice "+action.name());
		for(CentroidCluster<MultDimData> e:cResult){
			output.append("ClusterCentroid: " + Arrays.toString(e.getCenter().getPoint())+", "+"\n");
			//output.append("Cluster: " + StringUtils.join(e.getPoints(),",")+"\n");
		}
		for(CentroidCluster<MultDimData> e:cResult){
			//output.append("ClusterCentroid: " + Arrays.toString(e.getCenter().getPoint())+", "+"\n");
			output.append("Cluster: " + StringUtils.join(e.getPoints(),",")+"\n");
		}
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
	 */
	public void EPTestTensorFactor(String[] args,int start){
		 //tensor factor, 4-order,
		 //(actor,actor,action,month): counts
		 //run_single_bias_sals.sh
		 //[training] [output] [M] [Tout] [Tin] [N] [K] [C] [useWeight] [lambda]  [I1] [I2] ... [IN] [test] [query]
		
		 //training  output M  N K [I1] [I2] ... [IN]
		
		 String training = args[start];
		 String output = args[start+1];
		 String M = args[start+2];
		 
		 String N =args[start +3];
		 String K = args[start + 4];
		 
		 //I1,I2,I3,I4
		 String[] len = new String[Integer.valueOf(N)];
		 for(int i=0;i<len.length;i++){
			 len[i] = args[start+5+i];
		 }		 
		 String Tout = "10";
		 String Tin = "1";		 
		 String C = "1";
		 String useWeight ="0";
		 String lambda ="0.1";
		 
		 String[] tensorParams = new String[10+len.length];
		 assignParams(tensorParams,training,output,M,Tout,Tin,N,K,C,useWeight,lambda,len);
		 
		 boolean useBias = false;
		 		 
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
	private void assignParams(String[] tensorParams, String training, String output, String m, String tout, String tin,
			String n, String k, String c, String useWeight, String lambda, String[] len) {
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
		for(int i=0;i<len.length;i++){
			tensorParams[10+i]=len[i];
		}
	}
}
