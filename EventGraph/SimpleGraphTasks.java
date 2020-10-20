package EventGraph;

import org.graphstream.algorithm.measure.AbstractCentrality.NormalizationMode;
import org.graphstream.algorithm.measure.ClosenessCentrality;
import org.graphstream.algorithm.measure.DegreeCentrality;
import org.graphstream.algorithm.measure.EigenvectorCentrality;
import org.graphstream.algorithm.generator.DorogovtsevMendesGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.graphstream.algorithm.BetweennessCentrality;
import org.graphstream.algorithm.ConnectedComponents;
import org.graphstream.algorithm.PageRank;
import org.graphstream.algorithm.Spectrum;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.algorithm.community.EpidemicCommunityAlgorithm;
import org.graphstream.graph.Node;

import com.google.common.collect.Lists;

import org.graphstream.algorithm.measure.ConnectivityMeasure;
import org.graphstream.graph.Graph;

public class SimpleGraphTasks {
	
	public static final String STYLE = "node {" + "fill-mode: dyn-plain;"
			+ "fill-color: blue,yellow;" + "size-mode: dyn-size;"
			+ "stroke-color: black;" + "stroke-width: 1px;"
			+ "stroke-mode: plain;" + "}";
	
	/**
	 * constructor
	 */
	public SimpleGraphTasks() {
		System.setProperty("org.graphstream.ui", "javafx");
		//g.setAttribute("ui.quality");
		//g.setAttribute("ui.antialias");
		//g.setAttribute("ui.stylesheet", STYLE);
	}
	
	/**
	 * centrality measure
	 * @author quanyongf
	 *
	 */
	public enum measureChoice{
		ClosenessCentrality,
		DegreeCentrality,
		EigenvectorCentrality,
		BetweennessCentrality,
	};
	
	/**
	 * graph measures
	 * @param g
	 * @param attribute 
	 */
	public void graphMeasure(Graph g, measureChoice choice, String attribute) {
		

		
		
		//Map<String,Float> outMeasures = new HashMap<String,Float>();
		//ConnectivityMeasure cm = new ConnectivityMeasure();
		
		//int EdgeConnectivityNum = ConnectivityMeasure.getEdgeConnectivity(g);
		//outMeasures.put("EdgeConnectivityNum",(float) EdgeConnectivityNum);
		 
		switch(choice){
		
		case ClosenessCentrality:{

		ClosenessCentrality cc = new ClosenessCentrality(attribute,
				NormalizationMode.MAX_1_MIN_0, true, true);
		cc.init(g);
		cc.compute();
		 break;
		}
		
		case DegreeCentrality:{
		DegreeCentrality dc = new DegreeCentrality(attribute, NormalizationMode.MAX_1_MIN_0);
		dc.init(g);
		dc.compute();
		 break;
		}

		case EigenvectorCentrality:{
		EigenvectorCentrality ec = new EigenvectorCentrality(attribute, NormalizationMode.MAX_1_MIN_0);
		ec.init(g);
		ec.compute();
		break;
		}
		
		case BetweennessCentrality:{
		BetweennessCentrality bcb = new BetweennessCentrality(attribute);
		bcb.init(g);
		bcb.compute();
		break;
		}		
		}
		
		for (int i = 0; i < g.getNodeCount(); i++)
			g.getNode(i).setAttribute("graphMeasure",
					g.getNode(i).getNumber(attribute) * 25 + 5);

		//g.display();
		
		
	}
	
	/**
	 * graph connected components
	 * @param graph
	 */
	public void connectedComponents(Graph g) {
		//g.setAttribute("ui.quality");
		//g.setAttribute("ui.antialias");
		//g.setAttribute("ui.stylesheet", STYLE);
		
		ConnectedComponents ccm = new ConnectedComponents();
		ccm.init(g);
		ccm.compute();
		
//		for (int i = 0; i < g.getNodeCount(); i++)
//			g.getNode(i).setAttribute("ui.size",
//					g.getNode(i).getNumber("ui.color") * 25 + 5);
//
//		g.display();
	}
	
	/**
	 * pagerank
	 * @param graph
	 * @throws Exception
	 */
	public void PageRank(Graph graph) throws Exception {
		
		PageRank pageRank = new PageRank();
		pageRank.setVerbose(true);
		pageRank.init(graph);
		

		DorogovtsevMendesGenerator generator = new DorogovtsevMendesGenerator();
		generator.setDirectedEdges(true, true);
		generator.addSink(graph);
		
		generator.begin();
		while (graph.getNodeCount() < 100) {
			generator.nextEvents();
			for (Node node : graph) {
				double rank = pageRank.getRank(node);
				node.setAttribute("ui.size",
						5 + Math.sqrt(graph.getNodeCount() * rank * 20));
				node.setAttribute("ui.label",
						String.format("%.2f%%", rank * 100));
			}
			Thread.sleep(1000);
		}
	}
	
	/**
	 * 
	 * @param g
	 * @return
	 */
	public List<Double> toolkit4clusteringCoefficient(Graph g) {
		
		 List<Double> ccl = Lists.newArrayList();
		
		Iterator<Node> ier = g.iterator();
		while(ier.hasNext()) {
			double ccs = Toolkit.clusteringCoefficient(ier.next());
			ccl.add(ccs);
		}
		return ccl;
	}
	
	/**
	 * maximum clique
	 * @param g
	 * @return
	 */
	public List<Node> toolkit4MaximumClique(Graph g) {
		
		int cliqueCount = 0;
		int totalNodeCount = 0;
		List<Node> maximumClique = new ArrayList<Node>();
		for (List<Node> clique : Toolkit.getMaximalCliques(g)) {
			
			
			cliqueCount++;
			totalNodeCount += clique.size();
			if (clique.size() > maximumClique.size())
				maximumClique = clique;
		}
		return maximumClique;
	}
	
	/**
	 * community detection
	 * @param g
	 */
	public void communityDetection(Graph g) {
		
		EpidemicCommunityAlgorithm ec = new EpidemicCommunityAlgorithm();
		ec.init(g);
		ec.compute();
		
	}
	
	/**
	 * node degree distribution
	 * @param g
	 * @return
	 */
	public List<Integer> degrees(Graph g){
		
		List<Integer> list = Lists.newArrayList();
		
		
		Iterator<Node> nodess = g.iterator();
		while(nodess.hasNext()) {
			Node n = nodess.next();
			list.add(n.getDegree());
		}
		return list;
	}
	
	/**
	 * graph eigenvalues
	 * @param g
	 * @return
	 */
	public double[] graphSpectrum(Graph g) {
		Spectrum s = new Spectrum();
		s.init(g);
		s.compute();
		return s.getEigenvalues();
	}
}
