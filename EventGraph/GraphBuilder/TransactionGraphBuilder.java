package EventGraph.GraphBuilder;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.graphstream.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import util.Util.StringUtil;

public class TransactionGraphBuilder {
	
	private static final Logger logger = LoggerFactory.getLogger(TransactionGraphBuilder.class);
	//number of fields
	//public final static int NumTransFields =5;

	public final static boolean actorOnly = true;

	public static void main(String[] args){
		
		//transaction, event, actor1, actor2
		String transFile = args[0];
		//graph store file
		String graphStorefile = args[1];
		
		try {
			//readinto the graph
			List<ArrayList<Integer>> items = load2Mem(transFile);
			graphStreamIngest gi = new graphStreamIngest();
			Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
			gi.buildTransactionGraph(graph, items);
			
			graph.write(graphStorefile);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	} 
		/**
		 * build
		 * @param transFile
		 * @param graphStorefile
		 */
		public static void build(String transFile,String graphStorefile) {
 
			
			//transaction, event, actor1, actor2
			//String transFile = args[0];
			//graph store file
			//String graphStorefile = args[1];
			
			try {
				//readinto the graph
				List<ArrayList<Integer>> items = load2Mem(transFile);
				graphStreamIngest gi = new graphStreamIngest();
				Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
				gi.buildTransactionGraph(graph, items);
				
				//write graph
				graph.write(graphStorefile);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} 
		
	
	/**
	 * 
	 * @param transFile: fileTestTensor_EventItemsIndex
	 * globalcode+","+indexFrom+","+indexTo+","+indexEvent+","+indexDate+","+reported
	 * @return
	 * @throws Exception
	 */
	private static List<ArrayList<Integer>> load2Mem(String transFile) throws Exception {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new FileReader(transFile));
			List<ArrayList<Integer>> all = new ArrayList<ArrayList<Integer>>();
			String delim =",";
			while(true){
				String line = br.readLine();
				if(line==null)
					break;		
				
				String[] tokens = line.split(delim);
				ArrayList<Integer> list = new ArrayList<Integer>();
				//event, actor1, actor2
				for(int i=0;i<tokens.length;i++) {
					list.add(Integer.valueOf(tokens[i]));			
				}
				all.add(list);
				
			}
			br.close();
			return all;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static Map<Integer,List<Integer>> load2Mem4Event(String transFile) throws Exception {
		// TODO Auto-generated method stub
		try {
			BufferedReader br = new BufferedReader(new FileReader(transFile));
			
			Map<Integer,List<Integer>> out  = Maps.newHashMap();
			
			
			String delim =",";
			while(true){
				String line = br.readLine();
				if(line==null)
					break;		
				
				String[] tokens = line.split(delim);
				
				Integer eid = Integer.valueOf(tokens[0]);
				
				
				ArrayList<Integer> list = new ArrayList<Integer>();
				//event, actor1, actor2
				for(int i=0;i<tokens.length;i++) {
					list.add(Integer.valueOf(tokens[i]));			
				}
				
				
				out.put(eid, list);
				
			}
			br.close();
			return out;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	
	/**
	 * add label
	 * @param inputFile
	 * @param outputFile
	 * @param indexNodeTable
	 * @param indexActionTable
	 * @param indexDateTable
	 */
	public static void build(String transFile, String graphStorefile,
			Map<Integer, String> indexNodeTable,
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable,boolean actorOnly) {
		// TODO Auto-generated method stub
		try {
			//readinto the graph
			List<ArrayList<Integer>> items = load2Mem(transFile);
			logger.info("event list size: "+items.size());
			graphStreamIngest gi = new graphStreamIngest();
			
			Graph graph = graphStreamIngest.getGraph(graphStreamIngest.
					graphType,transFile);
			
			/**
			 * default, remove event
			 */
			if(actorOnly) {
				gi.buildActorTransactionGraph(graph, items,indexNodeTable, 
						indexActionTable, indexDateTable);
			}else {
				gi.buildTransactionGraphEventActorAll(graph, items,indexNodeTable, 
						indexActionTable, indexDateTable);
			}
			
			
			
			
			//write graph
			graph.write(Boolean.toString(actorOnly).concat(graphStorefile));
			items.clear();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
