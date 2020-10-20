package util.Util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.graphstream.algorithm.Dijkstra;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.AdjacencyListGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class GraphStreamUtil {

	 private static final Logger logger = LoggerFactory.getLogger(GraphStreamUtil.class);
	
	 public static int sizeof(Object obj) throws IOException {
	        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
	        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);

	        objectOutputStream.writeObject(obj);
	        objectOutputStream.flush();
	        objectOutputStream.close();

	        return byteOutputStream.toByteArray().length;
	    }
	 
	 
	 /**
	 * build subgraph
	 * @param g
	 * @param nodes
	 * @return
	 */
	public static Graph subGraph(Graph g,Set<Node> nodes) {
		//create a new node
		Graph subg = new AdjacencyListGraph("subGraph".concat(Integer.toString(g.getNodeCount())));//new AdjacencyList(n);
		//add node
		for(Node node:nodes) {
			subg.addNode(node.getId());			
		}
		//add edge
		for(Node node:nodes) {
			//visit the edge
			Iterator<Edge> edgesIter = g.getNode(node.getId()).edges().iterator();
			while(edgesIter.hasNext()) {
				Edge one = edgesIter.next();
				if(nodes.contains(one.getSourceNode())&&nodes.contains(one.getTargetNode())){
					String id="subGraph".concat(one.getId());
					if(subg.getEdge(id)!=null) {
						logger.info("edge existed: "+one.getId());
					}else {
						Edge edge = subg.addEdge(id, 						
							one.getSourceNode().getId(),one.getTargetNode().getId());
						edge.setAttribute("length", one.getNumber("length"));
					}
				}
			}
		}//end
	return subg;
	}
	
	/**
	 * traverse on the graph
	 * @param g
	 * @return
	 */
	public  static  double[][] Dijkstra(Graph graph,String attrib){
		
		Iterator<Node> nodeIter = graph.iterator();
		Map<String,Integer> mapper = Maps.newHashMap();
		int index0 = 0;
		while(nodeIter.hasNext()) {
			Node currentNode= nodeIter.next();
			mapper.put(currentNode.getId(),index0);
			index0++;
		}
		
		int r = graph.getNodeCount();
		logger.info("nodeCount: "+r);
		double[][] out = new double[r][];
		for(int i=0;i<r;i++) {
			out[i] = new double[r];
		}
		
		Dijkstra da = new Dijkstra(Dijkstra.Element.EDGE, "result", attrib);
		da.init(graph);
		
		Iterator<Node> ier = graph.iterator();
		while(ier.hasNext()) {
			Node me = ier.next();
			da.setSource(me);
			da.compute();
			//iterate
			for (String nodeID : mapper.keySet()) {
				Node  node2 = graph.getNode(nodeID);
				double len=da.getPathLength(node2);
				//System.out.printf("%s->%s:%6.2f%n", da.getSource(), node2, len);
				int from = mapper.get(me.getId());//Integer.parseInt(me.getId().substring(5));
				int to = mapper.get(node2.getId()); //Integer.parseInt(node2.getId().substring(5));
				out[from][to]= len;
			}
		}//end
		mapper.clear();
		return out;
		
	}

	/**
	 * get graph node-id
	 * @param subGraph
	 * @return
	 */
	public static int[] subGraphIndex(Graph subGraph) {
		// TODO Auto-generated method stub
		int[] idx = new int[subGraph.getNodeCount()];
		Iterator<Node> ier = subGraph.nodes().iterator();
		int index = 0;
		while(ier.hasNext()) {
			Node one = ier.next();
			idx[index] = Integer.valueOf(one.getId());
			index++;
		}
		return idx;
	}
}
