package EventGraph.GraphBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.graphstream.graph.Graph;

import util.Util.TripleGP;

public class SimilarityGraphBuilder {

	public static void main(String[] args){
		
		//actor1, actor2, similarity
		String similarityFile = args[0];
		float threshold =  Float.valueOf(args[1]);
		
		String graphoutFile = args[2];
		
		try {
			List<TripleGP<Integer,Integer,Float>> items = load2Mem(similarityFile);
			graphStreamIngest gi = new graphStreamIngest();
			Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
			//get graph
			gi.buildEventSimilarityGraph(graph, items, threshold);
			//obtain the graph
			graph.write(graphoutFile);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	/**
	 * build graph
	 * @param similarityFile
	 * @param threshold
	 * @param graphoutFile
	 */
	public static void build(String similarityFile,String graphoutFile,float threshold){
		
		//actor1, actor2, similarity
		//String similarityFile = args[0];
		//float threshold =  Float.valueOf(args[1]);
		
		//String graphoutFile = args[2];
		
		try {
			List<TripleGP<Integer,Integer,Float>> items = load2Mem(similarityFile);
			graphStreamIngest gi = new graphStreamIngest();
			Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
			//get graph
			gi.buildEventSimilarityGraph(graph, items, threshold);
			//obtain the graph
			graph.write(graphoutFile);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * similarity
	 * @param similarityFile
	 * @throws Exception 
	 */
	private static List<TripleGP<Integer,Integer,Float>> load2Mem(String similarityFile) throws Exception {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new FileReader(similarityFile));
			List<TripleGP<Integer,Integer,Float>> all = new ArrayList<TripleGP<Integer,Integer,Float>>();
			String delim =",";
			while(true){
				String line = br.readLine();
				if(line==null)
					break;		
				
				String[] tokens = line.split(delim);
				//from
				int from = Integer.valueOf(tokens[0]);
				//to
				int to = Integer.valueOf(tokens[1]);
				//similarity
				float similarity = Float.valueOf(tokens[2]);
				all.add(new TripleGP<Integer,Integer,Float>(from,to,similarity));
				
			}
			br.close();
			return all;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * build the graph by similarity
	 * @param inputFile
	 * @param outputFile
	 * @param threshold
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public static void build(String transFile,String similarityFile, 
			String graphoutFile, float threshold,
			Map<Integer, String> indexNodeTable,
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable) {
		// TODO Auto-generated method stub
		try {
			
			Map<Integer,List<Integer>> EventLabels = TransactionGraphBuilder.load2Mem4Event(transFile);
			
			List<TripleGP<Integer,Integer,Float>> items = load2Mem(similarityFile);
			graphStreamIngest gi = new graphStreamIngest();
			Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
			//get graph
			gi.buildEventSimilarityGraph(graph, EventLabels,items, threshold,
					indexNodeTable,indexActionTable,indexDateTable);
			//obtain the graph
			graph.write(graphoutFile);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
