package EventGraph.graphStream.RepresentationLearning.mmdw.Learn;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.graphstream.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import EventGraph.EventGraphMining;
import EventGraph.graphStream.RepresentationLearning.tsne.TSneUtils;

public class MMDW {

	private static final Logger logger = LoggerFactory.getLogger(MMDW.class);
	
	private static void printError() {
        System.err.println("java -jar MMDW graphFile modelFile alphaLevel dimension");
        System.err.println("graphFile: "+" input for the graph");
        System.err.println("modelFile: "+" directory for keeping results, and supervised learning");
        System.err.println("alphaLevel: "+" system parameters");
        System.err.println("dimension: "+" coordinate dimension");          
        System.err.println("output: "+"vector, svm_model, result");
        
	}
	/**
	 * main entry
	 * @param args
	 */
	public static void main(String[] args) {
		
		LearnW.test_switch=false;
		
		/**
		 * input graph file
		 */
		String input = args[0];	
		/**
		 * keyword of the graph
		 */
		LearnW.source = input;
		/**
		 * directory for keeping results, and supervised learning
		 */
		LearnW.modelFile =args[1];

		/**
		 * label file
		 */
		/**
		 * label for each vertex, supervised learning, start from 0: [index class]
		 * e.g. 0 0
		 */
		LearnW.labelFile=LearnW.modelFile+"/"+LearnW.source+"_category.txt";
		/**
		 * output model
		 */
		//LearnW.W_DW=LearnW.modelFile+LearnW.source+"_LINE.txt";
		makeDiretory();
		
		
		/**
		 * system parameters
		 * order_of_alphaBias, -3 by default
		 */
		LearnW.alphaLevel=Integer.valueOf(args[2]);
		/**
		 * coordinate dimension
		 */
		LearnW.dimension = Integer.valueOf(args[3]);
		
		
		/**
		 * normalized matrix
		 */
		Graph g = EventGraphMining.loadGraph(input);
		double[][] matrix = EventGraphMining.NormalizedAdjacencyMatrix(g);
		String path = input+"NormalizedAdjMat";
		try {
			writeResult(matrix,path);
			LearnW.dataNum = matrix.length;
			
			LearnW.linkGraphFile = path;
			/**
			 * main entry
			 */
			mmdwMain(LearnW.alphaLevel,LearnW.dimension);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	/**
	 * create temporary dir to keep the parameters
	 */
	private static void makeDiretory() {
		// TODO Auto-generated method stub
		String dirBias=LearnW.modelFile+"/Bias/";
		judgeDir(new File(dirBias));
		String dirVector=LearnW.modelFile+"/vector/";
		judgeDir(new File(dirVector));
		String dirResult=LearnW.modelFile+"/result/";
		judgeDir(new File(dirResult));
		String dirSVM=LearnW.modelFile+"/svm_model/";
		judgeDir(new File(dirSVM));
		
		
	}

	/**
	 * dir created if not existed
	 * @param dir
	 */
	public static void judgeDir(File dir) {
		if(dir.exists()) {
			if(dir.isDirectory()) {
				logger.warn("dir existed: "+dir);
			}else {
				logger.warn("The same file existed, dir not allowed");
			}
		}else {
			logger.info("make dir: "+dir);
			dir.mkdir();
		}
	}
	
	
	private static void mmdwMain(int alphaLevel,int dimension) throws Exception {
		
		StoreAlphaWeight.dimensionForSVM=dimension;//fixed
		// TODO Auto-generated method stub
		//********THREAD**********
				List<LearnW> lls=new ArrayList<LearnW>();
		    	for(int i=0;i<9;i++){
		    		LearnW ls=new LearnW();
		    		ls.lambda=0.3;
		    		ls.flagNum=i;
		    		ls.alpha=0.005;
		    		ls.alphaBias=25*Math.pow(10, alphaLevel);
		    		ls.limitRandom=0.1+0.1*i;//DIFFERENT PERCENTAGE OF TRAIN SET
		    		lls.add(ls);
		    	}
		    	System.out.println("Number of data : "+LearnW.dataNum);
		    	for(int i=0;i<9;i++){
					LearnW lsls1=lls.get(i);
					lsls1.start();
		    	}
	}
	/**
	 * output the matrix
	 * @param matrix
	 * @param path
	 * @throws IOException
	 */
	public static void writeResult(double[][] matrix, String path) throws IOException {
		// TODO Auto-generated method stub
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(path));
		
        for(int i=0; i<matrix.length; i++) {
        	for(int j=0;j<matrix[0].length;j++) {
        	//write	
            bw.write(Double.toString(matrix[i][j]));
            if(j!=matrix[0].length-1) {
            	bw.write(" ");
            }
        	}
        	bw.newLine();
        }
        bw.close();
	}
	
	
}
