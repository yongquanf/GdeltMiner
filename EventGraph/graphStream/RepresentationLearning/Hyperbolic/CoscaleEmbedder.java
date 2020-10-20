package EventGraph.graphStream.RepresentationLearning.Hyperbolic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import javax.swing.JFrame;

import org.apache.commons.math3.util.MathArrays;
import org.apache.commons.math3.util.ResizableDoubleArray;
import org.apache.hadoop.util.StringUtils;
import org.graphstream.algorithm.ConnectedComponents;
import org.graphstream.algorithm.Dijkstra;
import org.graphstream.algorithm.ConnectedComponents.ConnectedComponent;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.ui.view.Viewer;
import org.math.plot.FrameView;
import org.math.plot.Plot2DPanel;
import org.math.plot.PlotPanel;
import org.math.plot.plots.ScatterPlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;

import EventGraph.EventGraphMining;
import EventGraph.GraphBuilder.graphStreamIngest;
import EventGraph.IncrementalLearning.NC.Vec;
import EventGraph.graphStream.kcore.Import;
import util.DimReduce.IsoMap;
import util.Util.ArraySort;
import util.Util.GraphStreamUtil;
import util.Util.StringUtil;
import util.Util.powerLaw.Discrete;
//import util.tensor.Jama.Matrix;
//import util.tensor.Jama.SingularValueDecomposition;

import util.Util.smile.math.matrix.Matrix;
import util.Util.smile.math.matrix.SVD;
import util.Util.smile.math.Math;
import util.Util.smile.math.matrix.DenseMatrix;
import util.Util.smile.math.matrix.EVD;
import util.Util.smile.math.matrix.SparseMatrix;
/**
 * 
 * todo: powerlaw fit. 04-25
 * 
 * ref: Machine learning meets complex networks via coalescent embedding in the hyperbolic space
 * 
 * @author eric
 *
 */
public class CoscaleEmbedder {

	private static final Logger logger = LoggerFactory.getLogger(CoscaleEmbedder.class);
	
	public static final String STYLE = "node {" + "fill-mode: dyn-plain;"
			+ "fill-color: blue,yellow;" + "size-mode: dyn-size;"
			+ "stroke-color: black;" + "stroke-width: 1px;"
			+ "stroke-mode: plain;" + "}";
	
	/**
	 * synthetic
	 * @return
	 */
	public Matrix syntheticAdjacencyMatrix() {
		
		int m = 50;
		int n = 50;
		int deg = 10;
		//dense matrix
		DenseMatrix x = Matrix.zeros(m, n);
				
		for(int i=0;i<m;i++) {
			
			int degOne = Math.min(deg, (int) Math.round(Math.ceil(Math.random()*deg)));
			HashSet<Integer> neighbors = Sets.newHashSet();
			while(neighbors.size()<degOne) {
				int index = (int) Math.round(Math.floor(Math.random()*m));
				neighbors.add(index);
			}
			Iterator<Integer> ier = neighbors.iterator();
			while(ier.hasNext()) {
				int node = ier.next();
				x.set(i, node, 1);
			}
		}
		return x;
	}
	
	private static void printError() {
        System.err.println("java -jar CoscaleEmbedder graphFile adjFile delim");
        System.err.println("graphFile: "+" input for the graph");
        System.err.println("adjFile: "+" adjacency matrix for the graph");
        System.err.println("delim: "+" delim for parsing adj matrix, default: comma");
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
        		"		\"xml\", FileSinkGEXF;\n" + 
        		"		\"png\", FileSinkImages;\n" + 
        		"		\"jpg\", FileSinkImages;");
        System.err.println("threshold: "+" threshold for the similarity file"); 
        System.err.println("outputAnalysisFile: "+"write to the analysis");
        System.err.println("output: "+" Graph (for build) OR outputAnalysisFile+ (inputFile+Delta)");
        
	}
	
	
	public static void main(String[] args) {
		
		printError();
		
		System.setProperty("org.graphstream.ui", "javafx");
		
		//CoscaleEmbedder ce = new CoscaleEmbedder();
		
		//load graph
		String input = args[0];
		Graph g = EventGraphMining.loadGraph(input);
		//load adjacency matrix
		String adjFile = args[1];
		String delim = args[2];
		
		int[][] adjMat = null;
		try {
			adjMat = Import.loadLarge(adjFile, delim);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(adjMat==null) {
			logger.error("adj matrix load failed");
			return;
		}
		logger.info("nodeCount:" +g.getNodeCount()+", adj: "+adjMat.length+", "+adjMat[0].length);
		
		mineGraph(input,adjMat,g);
				
		
	}
	
	/**
	 * 2d 
	 * @param inputFile
	 * @param adjMat//each row: #neighbors;
	 * @param g
	 */
	public static Matrix mineGraph(String inputFile, int[][] adjMat0, Graph g0) {
		// TODO Auto-generated method stub

		CoscaleEmbedder newone = new CoscaleEmbedder();
		
		  
        // Use largest connected component.
        ConnectedComponents cc = new ConnectedComponents();
		cc.init(g0);
		cc.compute();
		ConnectedComponent maxNodeComponent = cc.getGiantComponent();
		
		logger.info("connected components, largest one has {} samples.", 
				maxNodeComponent.getNodeCount(), g0.getNodeCount());

		Graph g = GraphStreamUtil.subGraph(g0, maxNodeComponent.getNodeSet());
		
		
		
		
		//construct the adjacency matrix
		Map<String,Integer> mapper = EventGraphMining.ID2Index(g);
						
		int nodeCount = mapper.size();
		
		//SparseMatrix x = obtainSparseMatrix(nodes,adjMat);
		int[][] adjMat = EventGraphMining.AdjacencyMatrixInt(g);
		//Matrix x = new Matrix(adjMat);
		//int nodeCount = adjMat.length;
		
		Matrix x = Matrix.newInstance(nodeCount,nodeCount,adjMat);	
		
		int svdDim = 2;
		Matrix out = newone.coalescentEmbedding(nodeCount,g,x,svdDim);
		
		//logger.info("rows: "+out.nrows()+", columns: "+out.getColumnDimension());
		
		Matrix coord = newone.polartoCart(out);
		
		graphPlot(g,x,coord,out,mapper);
		
		EventGraphMining.storeGraph("Hyerbolic"+inputFile,g);
		
		if(false) {
		//display
			//displayResult(polartoCart(out).getArray()); 
		}
		
		return out;
		//logger.info(StringUtil.toString(out.getArray()));
	}



	/**
	 * @param polarCoord 
	 * @param g 
	 * @param mapper 
	 * @param x: adjacency 
	 * @param polartoCart: node coordinates
	 */
	public static void graphPlot(Graph graph, Matrix adjMat, 
			Matrix polartoCart, Matrix polarCoord, Map<String, Integer> mapper) {
		// TODO Auto-generated method stub
		
		graphStreamIngest gi = new graphStreamIngest();
		//Graph graph = graphStreamIngest.getGraph();
		graph.setAttribute("ui.quality");
		//graph.setAttribute("ui.antialias");
		graph.setAttribute("ui.stylesheet", STYLE);
		
		
		//build the graph
		gi.AssignHyperbolicCoords2Graph(graph,mapper, adjMat,polartoCart,polarCoord);
					 
		Viewer  viewer= graph.display();
		//viewer.replayGraph(graph);
		 
	}

	/**
	 * polar to cartesian mapping
	 * @param Y
	 * @return
	 */
	public Matrix polartoCart(Matrix Y){
		//first column, theta; second column rho
		int rows = Y.nrows();
		DenseMatrix theta = getColumn(Y,0,rows); 
		DenseMatrix rho = getColumn(Y,1,rows);
		
		Matrix x =rho.mul(cos(theta));
		Matrix y = rho.mul(sin(theta));
		
		return  mergeMatrixByColumn(x,y,theta.nrows());
	}
	
	/**
	 * element wise sin
	 * @param in
	 * @return
	 */
	private DenseMatrix sin(Matrix in) {
		// TODO Auto-generated method stub
		DenseMatrix out = Matrix.newInstance(in.nrows(), in.ncols(), 0);
		for(int i=0;i<in.nrows();i++) {
			for(int j=0;j<in.ncols();j++) {
				out.set(i, j, Math.sin(in.get(i, j)));
			}
		}
		return out;
	}

	/**
	 * element wise cos
	 * @param in
	 * @return
	 */
	private DenseMatrix cos(Matrix in) {
		// TODO Auto-generated method stub
		DenseMatrix out = Matrix.newInstance(in.nrows(), in.ncols(), 0);
		for(int i=0;i<in.nrows();i++) {
			for(int j=0;j<in.ncols();j++) {
				out.set(i, j, Math.cos(in.get(i, j)));
			}
		}
		return out;
	}

	static void displayResult(double[][] Y) {
		Plot2DPanel plot = new Plot2DPanel();

		ScatterPlot dataPlot = new ScatterPlot("Data", PlotPanel.COLORLIST[0], Y);
		plot.plotCanvas.setNotable(true);
		plot.plotCanvas.setNoteCoords(true);
		plot.plotCanvas.addPlot(dataPlot);

		FrameView plotframe = new FrameView(plot);
		plotframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		plotframe.setVisible(true);
	}
	
	
	/**
	 * x: adjacency matrix
	 * dim_red, dim reduction technique
	 * 	ISO: isomap, 2d,3d
	 *  'LE'    -> Laplacian Eigenmaps (valid for 2D and 3D)
	 *   'MCE'   -> Minimum Curvilinear Embedding (only valid for 2D)
	 *   'ncMCE' -> noncentered Minimum Curvilinear Embedding (only valid for 2D)
	 *   
	 * dims - dimensions of the hyperbolic embedding space, the alternatives are:
		%   2 -> hyperbolic disk
	 * @param g 

	 * @param x
	 * @param dim_redV
	 * @param angular_adjustment
	 * @param svdDim
	 * @return
	 */
	public Matrix coalescentEmbedding(int nodes,Graph g, Matrix x, int svdDim) {
		//angle
		Matrix coord1 = set_angular_coordinates_ISO_2D(g,x,svdDim);
		
		int rows = nodes;//coord1.nrows();
		
		logger.info("angular: "+coord1.nrows());
		//
		Matrix coord2 = set_radial_coordinates(x);
		
		logger.info("radial: "+coord2.nrows());
		
		//logger.info(Arrays.toString(coord2.getRowPackedCopy()));
		//return the rows*2 matrix as the coordinate
		return mergeMatrixByColumn(coord1,coord2,rows);
	}
	
	
	/**
	 * pack the matrix
	 * @param coord1
	 * @param coord2
	 * @param rowNum
	 * @return
	 */
	private static Matrix mergeMatrixByColumn(Matrix coord1, Matrix coord2,int rowNum) {
		// TODO Auto-generated method stub
		DenseMatrix out = Matrix.newInstance(rowNum,2,0);
		for(int i=0;i<rowNum;i++) {
			for(int c=0;c<out.ncols();c++) {
				//set
				if(c==0) {
					out.set(i, c, coord1.get(i, 0));
				}else {
					out.set(i, c, coord2.get(i, 0));
				}
			}
		}
		
		logger.info("$merge: "+out.nrows()+","+out.ncols());
		return out;
	}



	/**
	 * 
	 * @param x
	 * @return
	 */
	private Matrix set_radial_coordinates(Matrix x) {
		// TODO Auto-generated method stub
		
		int rows = x.nrows();
		//create range
		double[] gamma_range =CreateRanges(1.01,0.01,10.00);
		//double small_size_limit = 100;
		//create degree vector
		int[] deg=generateDegs(x);
		
		//fit power law
		 double gamma = plfitDiscrete(deg, gamma_range);
		 logger.info("pl fit: "+gamma);
		double beta = 1 / (gamma - 1);
		//sort by the decreasing degree
		 int[] idx= sort(deg,"descend");
		 // for beta > 1 (gamma < 2) some radial coordinates are negative
		 int n = x.nrows();
		 
		 DenseMatrix radial_coordinates = Matrix.zeros(n,1);
		 
		 for(int i=0;i<n;i++) {
			 radial_coordinates.set(idx[i],0,
			Math.max(0, 2*beta*Math.log(i+1) + 2*(1-beta)*Math.log(n)));
		 }
		 
		 return radial_coordinates;
		 
//		% fit power-law degree distribution
//		gamma_range = 1.01:0.01:10.00;
//		small_size_limit = 100;
//		if length(deg) < small_size_limit
//		    gamma = plfit(deg, 'finite', 'range', gamma_range);
//		else
//		    gamma = plfit(deg, 'range', gamma_range);
//		end
//		beta = 1 / (gamma - 1);
//
//		% sort nodes by decreasing degree
//		[~,idx] = sort(deg, 'descend');
//
//		% for beta > 1 (gamma < 2) some radial coordinates are negative
//		radial_coordinates = zeros(1, n);
//		radial_coordinates(idx) = max(0, 2*beta*log(1:n) + 2*(1-beta)*log(n));
	}

	/**
	 * descending sort
	 * @param deg
	 * @param string
	 * @return
	 */
	private int[] sort(int[] deg, String string) {
		// TODO Auto-generated method stub
		if(string.startsWith("descend")) {
			ArraySort as = new ArraySort();
			int[] degIndex=as.descendingSortReturnIndex(deg);
			return degIndex;
		}else {
			return null;
		}
	}

/**
 * powerlaw fit
 * @param deg
 * @param string
 * @param gamma_range
 * @return
 */
	private double plfitDiscrete(int[] deg, double[] gamma_range) {
		// TODO Auto-generated method stub
		
		Discrete distribution = Discrete.fit(Ints.asList(deg)).fit();
		return distribution.exponent();
		
		
	}


	/**
	 * outer degree
	 * @param x
	 * @return
	 */
	private int[] generateDegs(Matrix x) {
		// TODO Auto-generated method stub
		int[] ra = new int[x.nrows()];
		int n = ra.length;
		for(int i=0;i<ra.length;i++) {
			int c = positiveItemsINColumn(x,i);
			ra[i] = c;
		}
		return ra;
	}


	/**
	 * degree
	 * @param x
	 * @param rowIdx
	 * @return
	 */
	private int positiveItemsINColumn(Matrix x, int rowIdx) {
		// TODO Auto-generated method stub
		int c = 0;
		for(int j=0;j<x.ncols();j++) {
			if(x.get(rowIdx, j)>0) {
				c++;
			}
		}
		return c;
	}

	/**
	 * get elements
	 * @param starter
	 * @param interval
	 * @param ender
	 * @return
	 */
	private double[] CreateRanges(double starter, double interval, double ender) {
		// TODO Auto-generated method stub
		ResizableDoubleArray ra = new ResizableDoubleArray();
		double nowVal = starter;
		while(nowVal<=ender) {
			ra.addElement(nowVal);
			//increment
			nowVal+=interval;
		}
		return ra.getElements();
	}


	/**
	 * 
	 * @param g 
	 * @param x
	 * @param angular_adjustment
	 * @return
	 */
	
	public  Matrix set_angular_coordinates_ISO_2D(Graph g,Matrix x,int svdDim) {
		
		logger.info("iso embed");
		//dim red
		Matrix dr_coords = ISOMAP_SMILE(g,x,true,svdDim); //ISOMAP_propack(g,x, 2,true,svdDim);
		logger.info("iso ended");
		//logger.info("drCoord: "+StringUtil.toString(dr_coords.getArray()));
		
		//% from cartesian to polar coordinates
		//% using dimensions 1 and 2 of embedding
		int rows = dr_coords.nrows();
		
		//get the angle coordinate
		DenseMatrix ang_coords = getColumn( cart2pol(dr_coords),0,rows);
		//change angular range from [-pi,pi] to [0,2pi]
		ang_coords.mod(2*Math.PI);
		
		//logger.info(Arrays.toString(ang_coords.getRowPackedCopy()));
		
		return ang_coords;
	}

	/**
	 * cartesian to polar coordinates, first two columns
	 * @param dr_coords
	 * @return
	 */
	private Matrix cart2pol(Matrix dr_coords) {
		// TODO Auto-generated method stub
		
		//dr_coords: N*2
		
		
		//int i0=0;
		//int i1 = dr_coords.nrows()-1;
		int rows = dr_coords.nrows();
		
		DenseMatrix X1 = getColumn(dr_coords,0,rows);  //.getMatrix(i0, i1,0,0) ;
		DenseMatrix X2 = getColumn(dr_coords,1,rows);  //.getMatrix(i0, i1,1,1) ;
		//theta vector
		
		//rho vector
		Matrix theta = X2.tan2(X1);
		Matrix Rho = ((X1.mul(X1)).add(X2.mul(X2))).sqrt();
		
		
				
		return mergeMatrixByColumn(theta,Rho,Rho.nrows());
	}
	
	/**
	 * get the column, and return
	 * @param i
	 * @return
	 */
	public DenseMatrix getColumn(Matrix in,int columnIndex,int rows) {
			
			double[] rec = new double[rows];
			for(int i=0;i<rows;i++) {
				rec[i]=in.get(i,columnIndex);
			}
			//new instance
			DenseMatrix oneColumn = Matrix.newInstance(rec);			
			return oneColumn;		
	}
	

	/**
	 * 	%INPUT
		%   x => Distance or correlation matrix x
		%   n => Dimension into which the data is to be projected
		%   centring => 'yes' is x should be centred or 'no' if not
		%OUTPUT
		%   s => Sample configuration in the space of n dimensions
	 * @param g 
	 * @param x
	 * @param i
	 * @param b
	 * @return
	 */
	private  Matrix ISOMAP_propack0(Graph g, Matrix x, int n, 
			boolean centering,int svdDim) {
		// TODO Auto-generated method stub
		
		//% Iso-kernel computation
		DenseMatrix kernel = graphallshortestpaths(g,x);
		
		// Kernel centering
		if(centering) {
		    kernel=kernel_centering(kernel); //%Compute the centred Iso-kernel
		}

		// Embedding 
		//[~,L,V] = svd(kernel, 'econ');
		//sqrtL = sqrt(L(1:n,1:n)); clear L
		//V = V(:,1:n);
		//s = real((sqrtL * V')');
		
		
		
		SVD svd = kernel.svd(svdDim);
		
		//SingularValueDecomposition svd = new SingularValueDecomposition(kernel);
		
		int N = x.nrows();//.nrows();
		Matrix S = svd.getS();//.getMatrix(N, 0, n-1); //0,n-1
		DenseMatrix V = svd.getV();//.getMatrix(N, 0, n-1);
		//diag vector
		double[] diag = S.diag();
		for(int i=0;i<diag.length;i++) {
			diag[i] = Math.pow(diag[i],0.5);
		}
		//diag matrix
		DenseMatrix sqrtS = Matrix.diag(diag);
		
		
		//diag mult
		return (sqrtS.times(V.transpose())).transpose();
		
	}
	
	private  Matrix ISOMAP_SMILE(Graph g, Matrix x,  
			boolean centering,int svdDim) {
		// TODO Auto-generated method stub
		
		logger.info("SPT start");
		//% Iso-kernel computation
		DenseMatrix kernel = graphallshortestpaths(g,x);
		logger.info("SPT end");
		// Kernel centering
//		if(centering) {
//		    kernel=kernel_centering(kernel); //%Compute the centred Iso-kernel
//		}

		int n = kernel.nrows();
		int d = svdDim;
		

		 logger.info("centering start");
		//nearest neighbors
		double[][] D = kernel.array();
		//center
		 for (int i = 0; i < n; i++) {
	            for (int j = 0; j < i; j++) {
	                D[i][j] = -0.5 * D[i][j] * D[i][j];
	                D[j][i] = D[i][j];
	            }
	        }

		 
	        double[] mean = Math.rowMeans(D);
	        double mu = Math.mean(mean);

	        DenseMatrix B = Matrix.zeros(n, n);
	        for (int i = 0; i < n; i++) {
	            for (int j = 0; j <= i; j++) {
	                double b = D[i][j] - mean[i] - mean[j] + mu;
	                B.set(i, j, b);
	                B.set(j, i, b);
	            }
	        }

	        B.setSymmetric(true);


			 logger.info("centering end");
	        
	        
	        EVD eigen = B.eigen(d);
	        

	        if (eigen.getEigenValues().length < d) {
	            logger.warn("eigen({}) returns only {} eigen vectors", d, eigen.getEigenValues().length);
	            d = eigen.getEigenValues().length;
	        }

	        DenseMatrix V = eigen.getEigenVectors();
	        
	        logger.info("eig end");
	        
	        double[][] coordinates = new double[n][d];//new double[n][d];
	        
	        for (int j = 0; j < d; j++) {
	            if (eigen.getEigenValues()[j] < 0) {
	                throw new IllegalArgumentException(String.format("Some of the first %d eigenvalues are < 0.", d));
	            }

	            double scale = Math.sqrt(eigen.getEigenValues()[j]);
	            for (int i = 0; i < n; i++) {
	                coordinates[i][j] = V.get(i, j) * scale;
	            }
	        }        
		

			
		
		return Matrix.newInstance(coordinates);
		
		//diag mult
		//return (sqrtS.times(V.transpose())).transpose();
		
	}
	
	
	
	/**
	 * squared matrix, normalize
	 * @param kernel
	 * @return
	 */
	private DenseMatrix kernel_centering(DenseMatrix kernel) {
		// TODO Auto-generated method stub
		int N = kernel.nrows();//.nrows();
		
		DenseMatrix B = Matrix.ones(N,N);
		//identity matrix
		DenseMatrix identityMat = Matrix.eye(N);
		//create regularized matrix		
		DenseMatrix J = identityMat.sub(B.mul(1.0/N));
		
		DenseMatrix ksquared = (kernel.mul(kernel));
		
		
		DenseMatrix out=(J.times(ksquared.times(J))).mul(-0.5);
		return out;
	}

/**
 * compute the shortest path	
 * @param graph
 * @param x
 * @return
 */
public DenseMatrix graphallshortestpaths(Graph graph, Matrix x) {
		
		//build the graph
		//graphStreamIngest gi = new graphStreamIngest();
		
		//Graph graph = graphStreamIngest.getGraph();
		//build the graph
		//gi.buildTransactionGraph(graph, x);
		
		//graph.setAttribute("ui.quality");
		//graph.setAttribute("ui.antialias");
		//graph.setAttribute("ui.stylesheet", STYLE);
		 
		//Viewer  viewer= graph.display(true);
		//viewer.replayGraph(graph);
	
	Iterator<Node> nodeIter = graph.iterator();
	Map<String,Integer> mapper = Maps.newHashMap();
	int index0 = 0;
	while(nodeIter.hasNext()) {
		Node currentNode= nodeIter.next();
		mapper.put(currentNode.getId(),index0);
		index0++;
	}
	
		//shortest paths
		int r = graph.getNodeCount();
		logger.info("nodeCount: "+r);
		
		//store the pairwise distance
		//Matrix out = new Matrix(r,r);
		DenseMatrix out = Matrix.newInstance(r, r, 0);
		
		//dijkstra
		Dijkstra da = new Dijkstra(Dijkstra.Element.EDGE, "result", "length");
		da.init(graph);
		
		Iterator<Node> ier = graph.iterator();
		while(ier.hasNext()) {
			Node me = ier.next();
			da.setSource(me);
			da.compute();
			for (String nodeID : mapper.keySet()) {
				Node  node2 = graph.getNode(nodeID);
				double len=da.getPathLength(node2);
				//System.out.printf("%s->%s:%6.2f%n", da.getSource(), node2, len);
				int from = mapper.get(me.getId());//Integer.parseInt(me.getId().substring(5));
				int to = mapper.get(node2.getId()); //Integer.parseInt(node2.getId().substring(5));
				out.set(from, to, len);
			}
		}
	 
		 //graph.clear();
		 return out;
	}
	
	
	/**
	 * % ISOMAP for network embedding in a low dimensional space
	 * x: adjacency matrix
	 * m: dimension
	 * centering, yes or not
	 * @param x
	 * @param m
	 * @param directed
	 * @return
	 */
	public Matrix graphallshortestpaths(Matrix x) {
		
		//build the graph
		graphStreamIngest gi = new graphStreamIngest();
		
		Graph graph = graphStreamIngest.getGraph(graphStreamIngest.graphType);
		//build the graph
		gi.buildTransactionGraph(graph, x);
		
		//graph.setAttribute("ui.quality");
		//graph.setAttribute("ui.antialias");
		//graph.setAttribute("ui.stylesheet", STYLE);
		 
		//Viewer  viewer= graph.display(true);
		//viewer.replayGraph(graph);
	
		//shortest paths
		int r = graph.getEdgeCount();
		logger.info("edgeCount: "+r);
		//store
		DenseMatrix out = Matrix.newInstance(r, r, 0);//new Matrix(r,r);
		
		//dijkstra
		Dijkstra da = new Dijkstra(Dijkstra.Element.EDGE, "result", "length");
		da.init(graph);
		
		Iterator<Node> ier = graph.iterator();
		while(ier.hasNext()) {
			Node me = ier.next();
			da.setSource(me);
			da.compute();
			for (Node node2 : graph) {
				double len=da.getPathLength(node2);
				//System.out.printf("%s->%s:%6.2f%n", da.getSource(), node2, len);
				int from = Integer.parseInt(me.getId().substring(5));
				int to = Integer.parseInt(node2.getId().substring(5));
				out.set(from, to, len);
			}
		}
	 
		 //graph.clear();
		 return out;
	}
	
}
