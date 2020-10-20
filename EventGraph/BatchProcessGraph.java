package EventGraph;

import java.io.FileNotFoundException;

import org.graphstream.graph.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * warning: the process must be atomic, intermediate state may be unstable 
 * @author eric
 *
 */
public class BatchProcessGraph {

	private static final Logger logger = LoggerFactory.getLogger(BatchProcessGraph.class);
	
	public static void main(String[] args) {
		BatchProcessGraph test = new BatchProcessGraph();
		test.graphProcessor();
	}
	
	
	public void graphProcessor() {
		//sir monitor
		int spreader = 20;
		
	String[] lists0 = {			
			"20151010020161100.gexf",                     
			"2015110020154100.gexf",                      
			"2015410020157100.gexf",                     
			"20157100201510100.gexf",                     
			"20161010020171100.gexf",                     
			"2016110020164100.gexf",                      
			"2016410020167100.gexf",                      
			"20167100201610100.gexf",                     
			"20171010020181100.gexf",                     
			"2017110020174100.gexf",                      
			"2017410020177100.gexf",                      
			"20177100201710100.gexf",
			"2018410020187100.gexf",
			 "2018110020184100.gexf",
			 "20181010020191100.gexf",
			 "20187100201810100.gexf",
			 "2019110020194100.gexf"
	};
	
	String[] listts1 = {						
			 "2019110020194100.gml"
	};
	
	String[] lists = {	
			"true2015110020154100.gml",
			"true2015410020157100.gml" ,  			 
			"true20157100201510100.gml",  
			"true20151010020161100.gml", 
			"true2016410020167100.gml" ,  
			"true2016110020164100.gml",        
			"true20161010020171100.gml", 
			"true20167100201610100.gml"    ,      
			"true20177100201710100.gml",
			"true20171010020181100.gml"   ,  
			"true2017110020174100.gml"    ,
			"true2017410020177100.gml"     ,              
			"true20181010020191100.gml",            
			"true2018110020184100.gml",			           
			"true2018410020187100.gml",			                
			"true20187100201810100.gml",						       
			"true2019110020194100.gml"
	};
	
	try {
	for(int i=0;i<lists.length;i++) {
		processor(lists[i],spreader);
	}
	}
	catch(Exception e) {
		e.printStackTrace();		
	}
	}

	
	/**
	 * processor
	 * @throws FileNotFoundException 
	 */
	public void processor(String inputFile,int spreaders) throws FileNotFoundException {
		//perform graph analysis
		//String inputFile = args[7];
		
		//int spreaders = Integer.parseInt(args[8]);
		
		//graph
		Graph g = EventGraphMining.loadGraph(inputFile);
		
		String adjMatrixID = inputFile.concat("AdjacencyMatrixIdx");
		
		EventGraphMining.outputAdjacencyMatrixIndex(g,adjMatrixID);
		
		EventGraphMining.outputAdjacencyMatrixLabel(g,inputFile.concat("AdjacencyMatrixLabel"));
		
		EventGraphMining.outputAdjacencyMatrixIdx2Label(g, inputFile.concat("AdjacencyMatrixIdx2Label"));
		
		//adj file
		String adjFile = adjMatrixID;// inputFile.concat("AdjacencyMatrixIdx");
		String delim = ",";
		
		logger.info("Generate adjMatrix, Start mining");
		
		boolean result=true;
		//k-core mining
		result = Boolean.logicalAnd(result,EventGraphMining.KCoreMining(g,adjFile,delim, inputFile.concat("KCoreMining")));
		logger.info("KCoreMining completed!");
		
		//k-truss
		result = Boolean.logicalAnd(result,EventGraphMining.KTrussMining(g, 
				adjFile, delim, inputFile.concat("KTrussMining")));
		logger.info("KTrussMining completed!");
		//node anomaly
		int weight = 1;
		result = Boolean.logicalAnd(result,EventGraphMining.NodeAnomalyMining(true, 
				g, adjFile, delim, 
				inputFile.concat("NodeAnomalyMining"), weight));
		logger.info("NodeAnomalyMining completed!");
		//pagerank
		result = Boolean.logicalAnd(result,EventGraphMining.MultiThreadedPageRankMining(g, 
				adjFile, delim, inputFile.concat("MultiThreadedPageRankMining")));
		logger.info("MultiThreadedPageRankMining completed!");
		//influence
		if(false) {
		//too slow, skipped for now
		result = Boolean.logicalAnd(result,EventGraphMining.influenceNodeMining(g, 
				adjFile, delim,  
				inputFile.concat("influenceNodeMining"), spreaders));
		logger.info("influenceNodeMining completed!");
		}
		
		logger.info("All processor completed in: "+Boolean.toString(result));
		//store to graph
		if(result) {
			EventGraphMining.storeGraph(inputFile,g);
		}
		
		if(false) {
		//sir sim
			double _infectionRatio = 0.2;
			int _repetition = 5;
			EventGraphMining.SIRSimulation(g, adjFile, delim,
				inputFile.concat("SIRSimulation"), spreaders, _infectionRatio, _repetition);
			logger.info("SIRSimulation completed!");
		}
	}
}
