package EventGraph.graphStream.RepresentationLearning.Hyperbolic.PHyperbolic;

/**
 * 1. [For neighbors of a node, embed neigbors to a line, i.e., 1d embedding, 
 * sort them by the ascending order], random projection,
 
 * 2. [allocate theta uniform parts among neighbors]
 * 
 * Step2, compute theta between nodes, 
 * goal: 
 *  1)  nearby nodes have smaller angles; 
 *  2) hierarchically organization of nodes have smaller theta, 
 *  
 * Step: 
 * 1) build a tree on the graph, rooted at the source node
 * 2) ring-wise theta split
 *  2.1) outermost ring, split the range uniformly
 *  2.2) step to the next inner ring, merge parents
 *  2.3) move until the root
 * 3) obtain the angle set
 * 
 * REF:
 * On the Choice of a Spanning Tree for Greedy Embedding of Network Graphs

 * Embedding Metrics into Ultrametrics and Graphs into Spanning Trees with Constant Average Distortion
Scalable detection of statistically significant
communities and hierarchies, using message
passing for modularity
 * @author eric
 *
 */
public class PersonalizedTheta {
	
	/**
	 * a ball centered at the given point
	 * contains nodes within radii
	 */
	public void ball(int centerIdx,double radius) {
		
	}
	
	/**
	 * point chain:
	 * v1-v2-v3-v4-v5,
	 * v1-v2, nearest relation,
	 * similarity(v1,v2)>similarity(v2,v3)
	 */
	public void NodeChainSortByBallSimilarity() {
		
	}
	/**
	 * bfs, for each node, obtain the chain of each child
	 */
	public void recursiveNodeChain() {
		
	}
	
	/**
	 *a spanning tree minimizing 
	 *the distortion of the graph
	 */
	public void spanningTree() {
		
	}

	/**
	 * tree to angles
	 */
	public void treeToAngles() {
		
	}
}
