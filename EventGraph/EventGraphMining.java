package EventGraph;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.stream.file.FileSinkDGS;
import org.graphstream.stream.file.FileSinkDOT;
import org.graphstream.stream.file.FileSinkDynamicGML;
import org.graphstream.stream.file.FileSinkGEXF;
import org.graphstream.stream.file.FileSinkGML;
import org.graphstream.stream.file.FileSinkGraphML;
import org.graphstream.stream.file.FileSinkImages;
import org.graphstream.stream.file.FileSinkSVG2;
import org.graphstream.stream.file.FileSinkTikZ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AtomicDoubleArray;

import ETLPipeline.EventExtractStep;
import EventGraph.GraphBuilder.SimilarityGraphBuilder;
import EventGraph.GraphBuilder.TransactionGraphBuilder;
import EventGraph.LoaderFromFile.GraphLoadFromFile;
import EventGraph.graphStream.PageRank.MultiThreadedPageRank;
import EventGraph.graphStream.RepresentationLearning.Hyperbolic.CoscaleEmbedder;
import EventGraph.graphStream.RepresentationLearning.tsne.TSneBinaryDemo;
import EventGraph.graphStream.kcore.CoreDecomp;
import EventGraph.graphStream.kcore.Import;
import EventGraph.graphStream.kcore.TrussDecomp;
import EventGraph.graphStream.kcore.anomaly.CombineCoreA;
import EventGraph.graphStream.kcore.influence.IdentifySpreaders;
import EventGraph.graphStream.kcore.influence.SIRSimulation;
import util.Util.StringUtil;

import EventGraph.SimpleGraphTasks.measureChoice;

import util.Util.smile.math.matrix.Factory;
import util.Util.smile.math.matrix.DenseMatrix;
import util.Util.smile.math.matrix.Matrix;
public class EventGraphMining {

	private static final Logger logger = LoggerFactory.getLogger(EventGraphMining.class);
	
	
	public static final String STYLE = "node {" + "fill-mode: dyn-plain;"
			+ "fill-color: blue,yellow;" + "size-mode: dyn-size;"
			+ "stroke-color: black;" + "stroke-width: 1px;"
			+ "stroke-mode: plain;" + "}";
	
	/**
	 * output label
	 */
	public static boolean AdjMatOutLabel = true;
	
	
	/**
	 * constructor
	 */
	public EventGraphMining() {
	}
	
	/**
	 * graph miner
	 * @param args
	 */
	public static void main(String[] args) {
		
		printError();
		
		EventGraphMining test = new EventGraphMining();
		//mining type
		int selector = Integer.parseInt(args[0]);
		//build graph
		if(selector==1) {
			//file type
			boolean actorOnly = Boolean.parseBoolean(args[1]);
			//input file
			String transFile = args[2];
			
			String similarityFile = args[3];
			//write to graph file
			String outputFile = args[4];
			//similarity threshold
			float threshold = Float.parseFloat(args[5]);
			//reverse index
			String reverseIndexFile = args[6];
			
			 Map<Integer,String> indexNodeTable = Maps.newHashMap();
			 Map<Integer,String> indexActionTable = Maps.newHashMap();
			 Map<Integer,Date> indexDateTable = Maps.newHashMap();
			 //parse the event files
			 EventExtractStep.parseIndexJSON(reverseIndexFile,  
					 indexNodeTable,indexActionTable, indexDateTable);
			 logger.info("Items: entities: "+indexNodeTable.size()+
					 ", actions: "+indexActionTable.size()+
					 ", dates: "+indexDateTable.size());
			
			 //only event 
			boolean IsSimilarityOrNot = false;
			//build the graph
			test.buildGraph(IsSimilarityOrNot, transFile,similarityFile, 
					outputFile, threshold,
					indexNodeTable,indexActionTable,indexDateTable,actorOnly);
			
			

			 
		}else if(selector ==2) {
			//
			String inputFile = args[7];
			Graph g = EventGraphMining.loadGraph(inputFile);
			String outputAnalysisFile = args[8];
			try {
				test.performSimpleTasks(g,outputAnalysisFile);
				g.write(inputFile);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(selector ==3) {
			//
			String inputFile = args[7];
			Graph g = EventGraphMining.loadGraph(inputFile);
			//write to the
			try {			
				//if(!AdjMatOutLabel) {
					outputAdjacencyMatrixIndex(g,inputFile.concat("AdjacencyMatrixIdx"));
				//}else {
					outputAdjacencyMatrixLabel(g,inputFile.concat("AdjacencyMatrixLabel"));
					
					outputAdjacencyMatrixIdx2Label(g, inputFile.concat("AdjacencyMatrixIdx2Label"));
					
				//}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else if(selector ==4) {
			//perform graph analysis
			String inputFile = args[7];
			
			int spreaders = Integer.parseInt(args[8]);
			
			//graph
			Graph g = EventGraphMining.loadGraph(inputFile);
			//adj file
			String adjFile = inputFile.concat("AdjacencyMatrixIdx");
			String delim = ",";
			
			logger.info("Start mining");
			
			//k-core mining
			KCoreMining(g,adjFile,delim, inputFile.concat("KCoreMining"));
			logger.info("KCoreMining completed!");
			
			//k-truss
			KTrussMining(g, adjFile, delim, inputFile.concat("KTrussMining"));
			logger.info("KTrussMining completed!");
			//node anomaly
			int weight = 1;
			NodeAnomalyMining(true, g, adjFile, delim, 
					inputFile.concat("NodeAnomalyMining"), weight);
			logger.info("NodeAnomalyMining completed!");
			//pagerank
			MultiThreadedPageRankMining(g, adjFile, delim, inputFile.concat("MultiThreadedPageRankMining"));
			logger.info("MultiThreadedPageRankMining completed!");
			//influence
			influenceNodeMining(g, adjFile, delim,  
					inputFile.concat("influenceNodeMining"), spreaders);
			logger.info("influenceNodeMining completed!");
			
			//store to graph
			EventGraphMining.storeGraph(inputFile,g);
			
			//sir sim
			double _infectionRatio = 0.2;
			int _repetition = 5;
			SIRSimulation(g, adjFile, delim,
					inputFile.concat("SIRSimulation"), spreaders, _infectionRatio, _repetition);
			logger.info("SIRSimulation completed!");
		}else if(selector==5) {
			//tsne, input: factor matrix, row, columns
			int rows = Integer.parseInt(args[9]);
			int columns = Integer.parseInt(args[10]);
			String factorFile = args[11];
			logger.info("TSNE: rows: "+rows+", columns: "+columns+", "+factorFile);
			String[] argsTSNE= {"-asMatrix",args[9],args[10],factorFile};
			
			String out= factorFile+"tsne";
			
			tsneEmbed(argsTSNE,out);
			
		}
	}
	
	private static void printError() {
		 System.err.println("java -jar EventGraphMining selector ActorOnly inputFile similarityFile outputFile threshold reverseIndexFile inputGraphFile");
		        System.err.println("selector: "+"[1,2,3,4,5]. 1: build graph, 2: simple analysis, 3, extract adjacency matrix files, 4, graph mining, 5, tsne embedding");
		        System.err.println("ActorOnly: "+" true: only actor, false: actor + event");
		        System.err.println("inputFile: "+" input for the graph");
		        System.err.println("similarityFile: "+" event similarity for the graph");
		        System.err.println("outputFile: "+" graph store file, format, "
		        		+ "\"dgs\", FileSinkDGS;\n" + 
		        		"		\"dgsz\", FileSinkDGS;\n" + 
		        		"		\"dgml\", FileSinkDynamicGML;\n" + 
		        		"		\"gml\", FileSinkGML;\n" + 
		        		"		\"graphml\", FileSinkGraphML;\n" + 
		        		"		\"dot\", FileSinkDOT;\n" + 
		        		"		\"svg\", FileSinkSVG2;\n" + 
		        		"		\"pgf\", FileSinkTikZ;\n" + 
		        		"		\"tikz\", FileSinkTikZ;\n" + 
		        		"		\"tex\", FileSinkTikZ;\n" + 
		        		"		\"gexf\", FileSinkGEXF;\n" + 
		        		"		\"xml\", FileSinkGEXF;\n" );
		        System.err.println("threshold: "+" threshold for the similarity file"); 
		        System.err.println("reverseIndexFile: "+"metadata for vertices");
		        System.err.println("output: "+" Graph (for build) OR outputAnalysisFile+ (inputFile+Delta)");
		        System.err.println("inputGraphFile: "+" Graph for mining");        
	}
	
	
	/**
	 * Step 1, build
	 * @param similarFile
	 * @param outputFile
	 * @param threshold
	 * @param indexDateTable 
	 * @param indexActionTable 
	 * @param indexNodeTable 
	 */
	public void buildGraph(boolean isSimilarity,String transFile,
			String similarFile, String outputFile, 
			float threshold, Map<Integer, String> indexNodeTable, 
			Map<Integer, String> indexActionTable, 
			Map<Integer, Date> indexDateTable,boolean actorOnly) {
		if(isSimilarity) {
			SimilarityGraphBuilder.build(transFile,similarFile, outputFile, threshold,indexNodeTable, 
					indexActionTable, indexDateTable);
		}else {
			TransactionGraphBuilder.build(transFile, outputFile,indexNodeTable, 
					indexActionTable, indexDateTable,actorOnly);
		}
	}
	
	/**
	 * Step 2, load from graph
	 * @param graph
	 */
	public static Graph loadGraph(String graph) {
		return GraphLoadFromFile.loadFromGraph(graph);
	}
	
	/**
	 * write file to the adjacency matrix, one pair one line
	 * @param graph
	 * @throws FileNotFoundException 
	 */
	public static void outputAdjacencyMatrixIndex(Graph graph, String adjacencyFile) throws FileNotFoundException {
		
		PrintWriter out = new PrintWriter(adjacencyFile);		
		int m = graph.getNodeCount();
		for (int idx1 = 0; idx1 < m; idx1++) {
			Iterator<Node> ier = graph.getNode(idx1).neighborNodes().iterator();
			while(ier.hasNext()) {
				int idx2 = ier.next().getIndex();
				out.append(String.format("%d,%d\n", idx1,idx2));
			}
				out.flush();
			}
		out.close();
	}
	
	public static void outputAdjacencyMatrixLabel(Graph graph, String adjacencyFile) throws FileNotFoundException {
		
		PrintWriter out = new PrintWriter(adjacencyFile);		
		int m = graph.getNodeCount();
		for (int idx1 = 0; idx1 < m; idx1++) {
			Iterator<Node> ier = graph.getNode(idx1).neighborNodes().iterator();
			while(ier.hasNext()) {
				int idx2 = ier.next().getIndex();
				
				out.append(String.format("%s,%s\n", (String)graph.getNode(idx1).getAttribute("label"),
						(String)graph.getNode(idx2).getAttribute("label")));
			}
				out.flush();
			}
		out.close();
	}
	
	/**
	 * idx to token
	 * @param graph
	 * @param adjacencyFile
	 * @throws FileNotFoundException 
	 */
	public static void outputAdjacencyMatrixIdx2Label(Graph graph, String adjacencyFile) throws FileNotFoundException {
		
		PrintWriter out = new PrintWriter(adjacencyFile);		
		int m = graph.getNodeCount();
		for (int idx1 = 0; idx1 < m; idx1++) {
				out.append(String.format("%d,%s\n", idx1,(String)graph.getNode(idx1).
				getAttribute("label")));
			
				out.flush();
			}
		out.close();
	}
	
	/**
	 * get adjacency matrix
	 * @param graph
	 * @return
	 */
	public static RealMatrix AdjacencyMatrix(Graph graph) {
		int m = graph.getNodeCount();
		RealMatrix a = new Array2DRowRealMatrix(m, m);
		Edge e;

		for (int idx1 = 0; idx1 < m; idx1++)
			for (int idx2 = 0; idx2 < m; idx2++) {
				e = graph.getNode(idx1).getEdgeToward(idx2);
				a.setEntry(idx1, idx2, e != null ? 1 : 0);
			}

		return a;
	}
	
	public static int[][] AdjacencyMatrixInt(Graph graph) {
		int m = graph.getNodeCount();
		int[][] a = new int[m][m];
		for(int i=0;i<m;i++) {
			a[i]=new int[m];
		}
		Edge e;

		for (int idx1 = 0; idx1 < m; idx1++)
			for (int idx2 = 0; idx2 < m; idx2++) {
				e = graph.getNode(idx1).getEdgeToward(idx2);
				a[idx1][idx2]=e != null ? 1 : 0;
			}

		return a;
	}
	
	/**
	 * normalization
	 * @param graph
	 * @return
	 */
	public static double[][] NormalizedAdjacencyMatrix(Graph graph) {
		int m = graph.getNodeCount();
		double[][] a = new double[m][m];
		for(int i=0;i<m;i++) {
			a[i]=new double[m];
		}
		Edge e;

		for (int idx1 = 0; idx1 < m; idx1++) {
			double degree = graph.getNode(idx1).getDegree();
			for (int idx2 = 0; idx2 < m; idx2++) {
				e = graph.getNode(idx1).getEdgeToward(idx2);
				a[idx1][idx2]=e != null ? 1 : 0;
				if(degree!=0) {
					a[idx1][idx2]/=degree;
				}
			}
		}
		return a;
	}
	
	
	/**
	 * Step 3, perform simple tasks
	 * @param outputFile 
	 * @param graph
	 * @throws Exception 
	 */
	public void performSimpleTasks(Graph g, String outputFile) throws Exception {
		
		BufferedWriter outSimilarity = new BufferedWriter(new FileWriter(outputFile,true));
		outSimilarity.append("Graph: "+g.getNodeCount()+"\n");
		
		//List outcome = new ArrayList();
		
		SimpleGraphTasks st = new SimpleGraphTasks();
		//community
		st.communityDetection(g);
		//connected
		//st.connectedComponents(g);
		//measure
		st.graphMeasure(g, measureChoice.BetweennessCentrality,"ui.BetweennessCentrality");
		st.graphMeasure(g, measureChoice.ClosenessCentrality,"ui.ClosenessCentrality");
		st.graphMeasure(g, measureChoice.DegreeCentrality,"ui.DegreeCentrality");
		st.graphMeasure(g, measureChoice.EigenvectorCentrality,"ui.EigenvectorCentrality");
		//spectrum
		double[] spectrums = st.graphSpectrum(g);
		
		outSimilarity.append("graphSpectrum: "+Arrays.toString(spectrums)+"\n");
		
		//pagerank
		try {
			st.PageRank(g);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//clustering coefficient
		List<Double> cclist = st.toolkit4clusteringCoefficient(g);
		outSimilarity.append("clusteringCoefficient: "+ Arrays.toString(Doubles.toArray(cclist))+"\n");
		//maximum cliques
		List<Node> mcliques = st.toolkit4MaximumClique(g);
		outSimilarity.append("maximumCliques: "+StringUtil.toString(mcliques)+"\n");
		 outSimilarity.flush();
		 outSimilarity.close();
	}

	/**
	 * create the adjacency matrix
	 * @param g
	 * @return
	 */
	public static Matrix GetAdjacencyMatrix0(Graph g,
			Map<String,Integer> mapper) {
		// TODO Auto-generated method stub
		int nodes = g.getNodeCount();
		DenseMatrix x =  Factory.matrix(nodes,nodes,0);
		//keep id
		Iterator<Node> nodeIter = g.iterator();
		//Map<String,Integer> mapper = Maps.newHashMap();
		int index0 = 0;
		while(nodeIter.hasNext()) {
			Node currentNode= nodeIter.next();
			mapper.put(currentNode.getId(),index0);
			index0++;
		}
		//iterate
		nodeIter = g.iterator();
		while(nodeIter.hasNext()) {
			Node node= nodeIter.next();
			int meIndex = mapper.get(node.getId());
			//iterate
			Iterator<Node> ier = node.neighborNodes().iterator();
			while(ier.hasNext()) {
				Node you = ier.next();
				int yourIndex = mapper.get(you.getId());
				
				x.set(meIndex, yourIndex, 1);
			}
		}
		
		return x;
		
	}
	
	   /**
     * write the k-core label to the graph
     * 
     * @param graphFile
     */
	public static boolean UpdateAllNodesGraph( int[] kcOut, String attrib,Graph graph) {
		// TODO Auto-generated method stub
		
		if(kcOut!=null&&kcOut.length>0) {
		
		 Iterator<Node> ier = graph.iterator();
			while(ier.hasNext()) {			
				Node node = ier.next();
				int index = node.getIndex();
				if(index>=kcOut.length) {
					logger.warn("kcOut: "+kcOut.length+", "+index);
				}else {
					int kc = kcOut[index];
					
					node.setAttribute(attrib, kc);
				}
			}
			return true;
		}else {
			logger.warn("invalid attribute array");
			return false;
		}
			
	}
	/**
	 * 
	 * @param NodeIndex
	 * @param attrib
	 * @param graph
	 */
	public static boolean UpdateBoolean4NodesGraph( int[] NodeIndex, 
			String attrib,Graph graph) {
		
		if(attrib!=null&&NodeIndex!=null&&NodeIndex.length>0) {
			logger.info("update graph attributes");
			// TODO Auto-generated method stub
			// Iterator<Node> ier = graph.iterator();
			for(int i=0;i<NodeIndex.length;i++){	
					//node index
					int index = NodeIndex[i];
					//node
					Node node = graph.getNode(index);
					
					
					node.setAttribute(attrib, "True");
				}
			return true;
		}else {
			logger.warn("invalid attribute array");
			return false;
		}
		

			
	}
	
	
	/**
	 * k-core
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 */
	public static boolean KCoreMining(Graph g,
			String adjacencyFile, 
			String delim, String output) {
		//Graph g = loadGraph(g);
		
		//CoreDecomp kc = new CoreDecomp();

        int[][] adjMat;
		try {
			adjMat = Import.loadLarge(adjacencyFile, delim);
			
			if(adjMat==null) {
				return false;
			}
			
			logger.info("total: "+g.getNodeCount());
			
			 int[] kcOut = CoreDecomp.run(adjMat, true);
			 
			 logger.info("kcOut array: "+kcOut.length);
			 
			 CoreDecomp.export(adjacencyFile,kcOut, output, "\t");
			 //write to the graph
			 return UpdateAllNodesGraph(kcOut,"KCore",g);
			 
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       return false;
	}
	
	/**
	 * k-truss
	 * @param g
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 */
	public static boolean KTrussMining(Graph g,String adjacencyFile, 
			String delim, String output) {
		//Graph g = loadGraph(g);
		
		//TrussDecomp kc = new TrussDecomp();

        int[][] adjMat;
		try {
			adjMat = Import.loadLarge(adjacencyFile, delim);
			
			if(adjMat==null) {
				return false;
			}
			
			//k-truss
			 int[] kcOut = TrussDecomp.run(adjMat, true);
			 //write file
			 TrussDecomp.export(adjacencyFile,kcOut, output, "\t");
			 //write to the graph
			return UpdateAllNodesGraph(kcOut,"KTruss",g);
			 //EventGraphMining.storeGraph(g,g);
			 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
		return false;
	}
	
	/**
	 * k-core anomaly
	 * @param g
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 */
	public static boolean NodeAnomalyMining(boolean useTruss,Graph g,
			String adjacencyFile, String delim, String output,int weight) {
		
			//Graph g = loadGraph(g);
		try {
			int[][] adjMat = Import.loadLarge(adjacencyFile, delim);
			
			if(adjMat==null) {
				return false;
			}
			
			Set<Integer> anomalyNodes=  CombineCoreA.run(adjacencyFile,adjMat, output, 
					weight, useTruss);
			
			String arrrib = useTruss?"TrussAnomaly":"KCoreAnomaly";
			//update
			if(anomalyNodes!=null &&!anomalyNodes.isEmpty()) {
				 return UpdateBoolean4NodesGraph(Ints.toArray(anomalyNodes),arrrib,g);
				 //EventGraphMining.storeGraph(g,g);
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * influence node identify
	 * @param g2
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 */
	public static boolean influenceNodeMining(Graph g,String adjacencyFile, 
			String delim, String output, int spreaders) {
		//Graph g = loadGraph(g2);
		try {
			int[][] adjMat = Import.loadLarge(adjacencyFile, delim);
			
			if(adjMat==null) {
				return false;
			}
			
			IdentifySpreaders is = new  IdentifySpreaders();
			
			int[] nodes = IdentifySpreaders.run(adjMat,spreaders);
			//write
			IdentifySpreaders.writeResults(output, "\t", nodes);
			String arrrib = "Spreader";
			return UpdateBoolean4NodesGraph(nodes,arrrib,g);
			//EventGraphMining.storeGraph(g2,g);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * sir simulation
	 * @param g2
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 * @param _seeds
	 * @param _infectionRatio
	 * @param _repetition
	 */
	public static void SIRSimulation(Graph g,String adjacencyFile, 
			String delim, String output, 
			int _seeds, double _infectionRatio, int _repetition) {
		//Graph g = loadGraph(g2);
		try {
			int[][] adjMat = Import.loadLarge(adjacencyFile, delim);
			
			final int numOfSpreaders = _seeds;
	        final double infectedRatio = _infectionRatio;
	        final int repetition = _repetition;
	        final Random random = new Random();
	        //spreader seeds
	        int[] nodes = IdentifySpreaders.run(adjMat,_seeds);

	        /**
	         * influence of each node
	         */
	        EventGraph.graphStream.kcore.influence.SIRSimulation.run(adjMat, 
	        		nodes, output, infectedRatio, repetition, random);
	        			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param g2
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 * @param _dampingFactor
	 * @param _maxIterations
	 * @param _tolerance
	 * @param _thread
	 */
	public static boolean MultiThreadedPageRankMining(Graph g,String adjacencyFile, 
			String delim, String output) {
		//global params
		double _dampingFactor = 0.1;  
		int _maxIterations = 100;
		double _tolerance = 0.1; 
		int _thread = 4;
		
		
		//Graph g = loadGraph(g2);
		try {
			int[][] adjMat = Import.loadLarge(adjacencyFile, delim);
			
			if(adjMat==null) {
				return false;
			}
		
		double dampingFactor = _dampingFactor;
		int maxIterations  =   _maxIterations;
		double tolerance =  _tolerance;
		int threads =   _thread;
		
		/**
		 * vector 
		 */
		AtomicDoubleArray prv = MultiThreadedPageRank.PRM(g, output, 
				  dampingFactor, maxIterations, 
				 tolerance, threads);
		
		return UpdateAllNodesGraph(prv,"MultiThreadedPageRank",g);
		
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * update attribute for each node
	 * @param prv
	 * @param attrib
	 * @param graph
	 */
	private static boolean UpdateAllNodesGraph(AtomicDoubleArray prv, String attrib, Graph graph) {
		// TODO Auto-generated method stub
		if(prv!=null&&prv.length()>0) {
		Iterator<Node> ier = graph.iterator();
		while(ier.hasNext()) {			
			Node node = ier.next();
			int index = node.getIndex();
			double v = prv.get(index);
			node.setAttribute(attrib, v);
		}
		return true;
		}else {
			return false;
		}
	}
	
	/**
	 * hyperbolic
	 * @param GraphFile
	 * @param adjacencyFile
	 * @param delim
	 * @param output
	 */
	public static void HyperbolicRepresentLearning(String GraphFile,
			String adjacencyFile, 
			String delim) {
		
		Graph g = loadGraph(GraphFile);
		try {
			int[][] adjMat = Import.loadLarge(adjacencyFile, delim);
			/**
			 * hyperbolic embedding
			 */
			CoscaleEmbedder.mineGraph(GraphFile, adjMat, g);
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
	/**
	 * tsne embed
	 * -data double -asMatrix 1000 20 factorFile
	 * @param args
	 * @param out
	 */
	public static void tsneEmbed(String[]  args,String out) {
		 
		try {
			 			
			double [][] matrix = TSneBinaryDemo.loadDataTensor(args);
			if(matrix!=null) TSneBinaryDemo.runTSne(matrix);
			TSneBinaryDemo.writeResult(matrix,out);
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	

	/**
	 * save the graph
	 * @param input 
	 * @param g
	 */
	public static void storeGraph(String input, Graph g) {
		// TODO Auto-generated method stub
		try {
			g.write(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * id to index
	 * @return
	 */
	public static Map<String, Integer> ID2Index(Graph g) {
		// TODO Auto-generated method stub
		Map<String, Integer> out = Maps.newHashMap();
		int n = g.getNodeCount();
		for(int i=0;i<n;i++) {
			String id = g.getNode(i).getId();
			out.put(id, i);
		}
		return out;
	}
	
	
	
}
