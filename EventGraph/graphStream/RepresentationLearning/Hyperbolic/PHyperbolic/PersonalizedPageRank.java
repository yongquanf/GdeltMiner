package EventGraph.graphStream.RepresentationLearning.Hyperbolic.PHyperbolic;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.stream.Stream;

import org.apache.commons.math.util.MathUtils;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

import EventGraph.EventGraphMining;

/**
 * Step1: set radii, use personalized pagerank as it determines the distance to others, and dimensions 
 * the degree of stochastic block model,
 * 
 * FORA: Simple and Effective Approximate Single-Source Personalized PageRank
 * KDD 2017
 *  The basic idea of FORA is to
	combine two existing methods Forward Push (which is fast but
	does not guarantee quality) and Monte Carlo Random Walk (accurate but slow) in a simple and yet non-trivial way, leading to an
	algorithm that is both fast and accurate. Further, FORA includes
	a simple and effective indexing scheme, as well as a module for
	top-k selection with high pruning power.
 * @author eric
 *
 */
public class PersonalizedPageRank {
	private static final Logger logger = LoggerFactory.getLogger(
			PersonalizedPageRank.class);
	
	final static double constant = 2.5;
	
	//epsilon
	double epsilon = 0.5;
	//delta
	double delta = 0.01;
	//pf
	double pf = 0.01;
	
	final static int maxWalker =30;
	//forward push
	//double probabilityAlpha = 0.01; 
	
	//random walk len
	int randomWalkLen=30;
	//walk reset
	double RESET_PROBABILITY = 0.05;
	//graph
	Graph g;
	
	public static void main(String[] args) {
		
		int walkLen = 30;
		double reset = 0.05;
		PersonalizedPageRank ppr = new PersonalizedPageRank(walkLen,reset);
		String input = args[0];
		ppr.init(input);
		for(int i=0;i<100;i++) {
			int sourceIdx = i;
			double[] vec = ppr.compute(sourceIdx);
			logger.info(Arrays.toString(vec));
		}
	}
	
	/**
	 * 
	 * @param _randomWalkLen
	 * @param _RESET_PROBABILITY, reset the random walk
	 * @param _probabilityAlpha, terminate at a step for a random walk
	 */
	public PersonalizedPageRank(int _randomWalkLen,
			double _RESET_PROBABILITY) {			
		this.randomWalkLen = _randomWalkLen;
		this.RESET_PROBABILITY = _RESET_PROBABILITY;		
	}
	
	/**
	 * init graph
	 * @param input
	 */
	public void init(String input) {
		//load graph
		
		g = EventGraphMining.loadGraph(input);
		
		//delta
		this.delta = constant*1.0/g.getNodeCount();
		//pf
		this.pf = constant*1.0/g.getNodeCount();
		//epsilon
		this.epsilon = 0.5;
		
		logger.info("graph: "+g.getNodeCount()+", epsilon: "+
		this.epsilon+", delta: "+this.delta+", pf: "+this.pf+", randomWalk: "+this.randomWalkLen+
		", RESET_PROBABILITY: "+this.RESET_PROBABILITY);
	}
	
	/**
	 * compute the personalized graph
	 * @param sourceIdx
	 * @return
	 */
	public double[] compute(int sourceIdx) {

		
		int m = g.getNode(sourceIdx).getOutDegree();
		//residual threshold
		double residualThreshold = (this.epsilon/Math.sqrt(m))*
				Math.sqrt(this.delta/((2*this.epsilon/3.0+2)*Math.log(2.0/this.pf)));
		
		double[] pi =fora4WholeGraph(g,sourceIdx, this.RESET_PROBABILITY,residualThreshold);
		logger.info("PPR completed: "+pi.length);
		return pi;
	}
	
	/**
	 * output ppr
	 * @param source
	 */
	private double[] fora4WholeGraph(Graph g,int sourceIdx, double probabilityAlpha, 
			double residualThreshold) {
		//return residual and reserve
		//0:rs, 1: pis
		List<List<Double>> RetResidualAndReserve = forwardPush(g,sourceIdx, probabilityAlpha, 
				residualThreshold);
		List<Double> rss = RetResidualAndReserve.get(0);
		List<Double> pis = RetResidualAndReserve.get(1);
		//iterate
		int n = g.getNodeCount();
		double rsum = 0;
		for(int i=0;i<n;i++) {
			rsum +=rss.get(i);
		}
		double w = rsum *((2*epsilon/3.0 +2)*Math.log(2.0/pf))/(Math.pow(epsilon, 2)*delta);
		double[] pi = Doubles.toArray(pis);
		double[] rs = Doubles.toArray(rss);
		int rounds = 0;
		//iterate
		while(true) {
			boolean allNotPass=true;
			int selector = -1;
			for(int v=0;v<n;v++) {
				if(rs[v]>0) {
					allNotPass = false;
					selector = v;
					break;
				}
			}//end
			if(allNotPass || selector<0) {
				logger.info("finished "+rounds);
				break;
			}else {
				//find vi
				int vi = selector;
				double wi = Math.ceil(rs[vi]*w/rsum);
				double ai = (rs[vi]/rsum)*(w/wi);
				logger.info("vi: "+vi+", "+wi+", "+ai);
				
				Random r = new Random(System.currentTimeMillis());
				//random walk
				for(int i=1;i<=Math.min( maxWalker,wi);i++) {
					//generate random walk from the same node
					int t =randomWalk(g,vi,vi,randomWalkLen,r);
					logger.info("walker: "+t);
					pi[t]+=ai*rsum/w;
					
				}//end ppr
			}//end round
			
			rounds++;
		}
		return pi;
	}
	
	/**
	 * random walk on g, return terminal node
	 * @param g
	 * @param vi
	 * @param randomWalkLen2
	 * @param rnd 
	 * @return
	 */
	private int randomWalk(Graph g, int currentNode, int source,int randomWalkLen2, Random rnd) {
		// TODO Auto-generated method stub
		int numWalks = randomWalkLen2;
		int numOutEdge = g.getNode(currentNode).getOutDegree();
		if(numWalks>0) {
			//reset
			if(rnd.nextDouble()<RESET_PROBABILITY) {
				return randomWalk(g,source,source,numWalks,rnd);
			}else {
				//randomly select a neighbor
				
				int rndNum = Math.round(rnd.nextFloat()*numOutEdge)%numOutEdge;
				Node nextHop = g.getNode(currentNode).getLeavingEdge(rndNum).
						getTargetNode();
				//next hop
				return randomWalk(g,nextHop.getIndex(),source,numWalks-1,rnd);
			}
		}else {
			return currentNode;
		}
	}

	/**
	 * output pi, r
	 */
	private List<List<Double>> forwardPush(Graph g,int sourceIdx, double alpha, 
			double residualThreshold) {
		int n = g.getNodeCount();
		
		double[] rs = new double[n];
		double[] pis = new double[n];
		
		Arrays.fill(rs, 0);
		rs[sourceIdx]=1;
		Arrays.fill(pis, 0);
		logger.info("init: "+rs.length+", "+pis.length);
		int rounds = 0;
		//iterate
		while(true) {
			boolean allNotPass=true;
			int selector = -1;
			for(int v=0;v<n;v++) {
				float outDegree = g.getNode(v).getOutDegree();
				//trigger
				if(outDegree>0&&((rs[v]/(outDegree))>residualThreshold)) {
					allNotPass = false;
					selector = v;
					break;
				}
			}
			logger.info("traversed: "+n+", "+allNotPass+", "+selector);
			if(allNotPass || selector<0) {
				logger.info("finished "+rounds);
				break;
			}else {
				//continue
				Node v = g.getNode(selector);
				//iterator through outer edges
				Iterator<Edge> ier = v.edges().iterator();
				while(ier.hasNext()) {
					Edge one = ier.next();
					//directed or not
					if(one.isDirected()) {
						//src must be v
						if(one.getNode0().getIndex()==v.getIndex()) {
							Node uNode = one.getNode1();
							int u = uNode.getIndex();
							//scale by v
							rs[u]=rs[u]+(1-alpha)*(rs[v.getIndex()]/(v.getOutDegree()+0.0));
						}
					}else {
						//all nodes are useful
						if(one.getNode0().getIndex()==v.getIndex()) {
							Node uNode = one.getNode1();
							int u = uNode.getIndex();
							//scale by v
							rs[u]=rs[u]+(1-alpha)*(rs[v.getIndex()]/(v.getOutDegree()+0.0));
						}
					}
				}//end u
				//end pi
				pis[v.getIndex()] = pis[v.getIndex()] + alpha*rs[v.getIndex()];
				rs[v.getIndex()] = 0;
			}
			//end round
			rounds++;
		}
		
		List<List<Double>> out = Lists.newArrayList();
		out.add(Doubles.asList(rs));
		out.add(Doubles.asList(pis));
		
		
		return out;
	}
	
	
}
