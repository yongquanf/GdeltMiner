package EventCorrelationMining;

import java.io.File;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import ETLPipeline.EventExtractStep;
import ETLPipeline.DirParser.ETLDirApp;

public class ecMiner {

	private static final Logger logger = LoggerFactory.getLogger(ecMiner.class);
	public final static String outEventTopKAttributeFile ="_outEventTopKAttributeFile";
	
	/**
	 * mine the correlation
	 * @param args
	 */
	public static void main(String[] args) {
		
		printError();
		
		//int selector = Integer.parseInt(args[0]);
		//top k query
		//if(selector ==1) {
				
		int start =0;		
		int topK = Integer.parseInt(args[start]);
		 String training4TensorFactorization = args[start+1];
		 String factorFileDir = args[start+1+1];
		 
		int N = Integer.valueOf(args[start +2+1]);
		int K = Integer.valueOf(args[start + 3+1]);
		 
		 //I1,I2,I3,I4
		 int[] len = new int[Integer.valueOf(N)];
		 for(int i=0;i<len.length;i++){
			 len[i] = Integer.valueOf(args[start+i+4+1]);
		 }
		 
		 //event-itemset index
		 String EventItemIndexFile = training4TensorFactorization+
				 ETLDirApp.EventItemsIndex;//"_EventItemsIndex";
		 //name-index map
		 String reverseIndexFile = training4TensorFactorization+
				 ETLDirApp.reversePostfix;
		 
		 Map<Integer,String> indexNodeTable = Maps.newHashMap();
		 Map<Integer,String> indexActionTable = Maps.newHashMap();
		 Map<Integer,Date> indexDateTable = Maps.newHashMap();
		 //parse the event files
		 EventExtractStep.parseIndexJSON(reverseIndexFile,  indexNodeTable,indexActionTable, indexDateTable);
		 logger.info("Items: entities: "+indexNodeTable.size()+
				 ", actions: "+indexActionTable.size()+
				 ", dates: "+indexDateTable.size());
		  
		//out
		String outEventTopKPutFile=factorFileDir+File.separator+ 
				training4TensorFactorization+ecMiner.outEventTopKAttributeFile;
		
		TopKAttribeQuery tk = new TopKAttribeQuery();
		try {
			tk.topKAnalysis(factorFileDir, EventItemIndexFile, 
					outEventTopKPutFile, N, K, len, topK,indexNodeTable,
					indexActionTable,indexDateTable);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//}
	}
	/**
	 * output
	 */
	 private static void printError() {
	        System.err.println("java -jar ecMiner topK training4TensorFactorization factorFileDir "
	        		+ "N K {I1,I2,I3,I4...}");
	        System.err.println("topK: "+" k principle components per factor");
	        System.err.println("training4TensorFactorization "+"tensor factorization training data");
	        System.err.println("factorFileDir "+" factor file output dir"); 
	        System.err.println("N "+"dimension of the tensor, I1,...I4");
	        System.err.println("K "+"tensor rank");
	        System.err.println("{I1,I2,I3,I4,...} "+"factor size"); 
	        System.err.println("tensor-factorization principle-component output: "+ "EventItemIndexFile + outEventTopKPutFile");

	 }
	
}
