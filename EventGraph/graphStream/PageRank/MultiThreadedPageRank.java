/**
 * Copyright 2016 Twitter. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package EventGraph.graphStream.PageRank;

import com.google.common.primitives.Doubles;
import com.google.common.util.concurrent.AtomicDoubleArray;

import EventGraph.EventGraphMining;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongIterator;
import util.Util.StringUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;

/**
 * <p>
 * Multithreaded Implementation of PageRank. This implementation is the multi-threaded version of
 * {@link PageRank}, inheriting its design and thus its limitations also.
 * </p>
 *
 * <p>
 * This implementation partitions the adjacency lists by nodes and assigns each partition to a
 * thread during each iteration. Each partition distributes PageRank mass for the nodes that are
 * assigned to it. The PageRank vector is stored centrally as an {@link AtomicDoubleArray}, which
 * ensures all mutations are atomic, so concurrent access to the PageRank vector is appropriately
 * mediated.
 * </p>
 */
public class MultiThreadedPageRank {
  final private Graph graph;
  final private LongArrayList nodes;
  final private long maxNodeId;
  final private double dampingFactor;
  final private int nodeCount;
  final private int maxIterations;
  final private int threads;
  final private double tolerance;

  private double normL1 = Double.MAX_VALUE;
  private AtomicDoubleArray prVector = null;

  public static void printErr() {
	  System.err.println("java -jar MultiThreadedPageRank: inputFile "
	  		+ "outputFile dampingFactor maxIterations tolerance threads");
	  System.err.println("inputFile: "+" graph input");
	  System.err.println("dampingFactor: "+" pageRank paramer");
	  System.err.println("maxIterations: "+" pageRank paramer");
	  System.err.println("tolerance: "+" pageRank paramer");
	  System.err.println("threads: "+" system threads");
  }
  
  public static void main(String[] args) {
	  
	  	String inputFile = args[0];
		Graph g = EventGraphMining.loadGraph(inputFile);
		String outputFile = args[1];
		int start = 2;
		double dampingFactor = Double.parseDouble( args[start+1]);
		int maxIterations  = Integer.parseInt(args[start+2]);
		double tolerance = Double.parseDouble(args[start+3]);
		int threads = Integer.parseInt(args[start+4]);
		
		
		PRM(g,outputFile,dampingFactor,maxIterations,tolerance,threads);
		
		
  }
  
  /**
   * entry
   * @param g
   * @param outputFile
   * @param dampingFactor2
   * @param maxIterations2
   * @param tolerance2
   * @param threads2
   */
 public static AtomicDoubleArray PRM(Graph g, String outputFile, 
		  double dampingFactor, int maxIterations, 
		  double tolerance, int threads) {
	// TODO Auto-generated method stub
	  long maxNode = g.getNodeCount();
		LongArrayList nodes = new LongArrayList();
		//index of node index: [0,max-1]
		for(long i=0;i<maxNode;i++) {
			nodes.add(i);
		}
		
		MultiThreadedPageRank test = new MultiThreadedPageRank(g,nodes,maxNode,dampingFactor,
				maxIterations,tolerance,threads);
		int rounds = test.run();
		
		try {
			//write
			 
			BufferedWriter outPR = new BufferedWriter(new FileWriter(outputFile));
			outPR.append("config: "+Arrays.toString(new double[] {
					dampingFactor,maxIterations,tolerance			})+"\n");
			for(int i=0;i<maxNode;i++) {
				outPR.append(i+" "+test.prVector.get(i)+"\n");
			}
			outPR.flush();
			outPR.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.out.println(rounds+" "+StringUtil.toString(test.prVector));
		
		return test.prVector;
}

/**
   * Constructs this object for running PageRank over a directed graph.
   *
   * @param graph          the directed graph
   * @param nodes          nodes in the graph
   * @param maxNodeId      maximum node id
   * @param dampingFactor  damping factor
   * @param maxIterations  maximum number of iterations to run
   * @param tolerance      L1 norm threshold for convergence
   * @param threads        number of threads
   */
  public MultiThreadedPageRank(Graph graph, LongArrayList nodes,
                               long maxNodeId, double dampingFactor, int maxIterations,
                               double tolerance, int threads) {
    if (maxNodeId > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("maxNodeId exceeds Integer.MAX_VALUE!");
    }

    this.graph = graph;
    this.nodes = nodes;
    this.maxNodeId = maxNodeId;
    this.dampingFactor = dampingFactor;
    this.nodeCount = nodes.size();
    this.maxIterations = maxIterations;
    this.tolerance = tolerance;
    this.threads = threads;
  }

  private double computeL1Norm(AtomicDoubleArray a, AtomicDoubleArray b) {
    double ret = 0.0;
    for (int i = 0; i < a.length(); ++i) {
      ret += Math.abs(a.get(i) - b.get(i));
    }
    return ret;
  }

  private void iterate(double dampingAmount, LongArrayList noOuts, 
		  final LongArrayList[] nodePartitions) {
    AtomicDoubleArray nextPR = new AtomicDoubleArray((int) (maxNodeId + 1));

    // First compute how much mass is trapped at the dangling nodes.
    double dangleSum = 0.0;
    LongIterator iter = noOuts.iterator();
    while (iter.hasNext()) {
      dangleSum += prVector.get((int) iter.nextLong());
    }
    dangleSum = dampingFactor * dangleSum / nodeCount;
    final double d = dangleSum;

    // We use a CountDownLatch as a sync barrier to wait for all threads to finish on their
    // respective partitions.
    final CountDownLatch latch = new CountDownLatch(threads);

    // Start all the worker threads over each partition.
    for (int i=0;i<threads; i++ ) {
      new PageRankWorker(i, nextPR, nodePartitions[i], latch, dampingAmount + d).start();
    }
    // Note that an alternative implementation would be to use a CyclicBarrier so we don't need to
    // respawn new threads each time, but for a graph of any size, the cost of respawning new
    // threads is small relative to the cost the actual iterations.

    // Wait for all the threads to finish.
    try {
      latch.await();
    } catch (InterruptedException ex) {
      // Something bad happened, just abort.
      throw new RuntimeException("Error running PageRank!");
    }

    normL1 = computeL1Norm(prVector, nextPR);
    prVector = nextPR;
  }

  /**
   * Runs PageRank, either until the max number of iterations has been reached or the L1 norm of
   * the difference between PageRank vectors drops below the tolerance.
   *
   * @return number of iterations that was actually run
   */
  public int run() {
    LongArrayList noOuts = new LongArrayList();
    LongIterator iter = nodes.iterator();
    while (iter.hasNext()) {
      long v = iter.nextLong();
      //v as the id
      if (graph.getNode((int)v).getOutDegree() == 0) {
        noOuts.add(v);
      }
    }

    double dampingAmount = (1.0 - dampingFactor) / nodeCount;
    prVector = new AtomicDoubleArray((int) (maxNodeId + 1));
    
    //nodes.forEach(v -> prVector.set((int) (long) v, 1.0 / nodeCount));
    for(Long v:nodes) {
    	prVector.set((int) (long) v, 1.0 / nodeCount);
    }
    
    // We're going to divide the nodes into partitions, one for each thread.
    LongArrayList[] nodePartitions = new LongArrayList[threads];
    int partitionSize = nodes.size() / threads;
    for (int i=0; i<threads; i++) {
      int startPos = i * partitionSize;
      // The final partition get the rest of the nodes.
      int endPos = i == (threads - 1) ? nodes.size() : (i + 1) * partitionSize;
      nodePartitions[i] = new LongArrayList(nodes.subList(startPos, endPos));
    }

    int i = 0;
    while (i < this.maxIterations && normL1 > tolerance) {
      iterate(dampingAmount, noOuts, nodePartitions);
      i++;
    }

    return i;
  }

  /**
   * Returns the final L1 norm value after PageRank has been run.
   *
   * @return the final L1 norm value after PageRank has been run
   */
  public double getL1Norm() {
    return normL1;
  }

  /**
   * Returns the PageRank vector, or null if PageRank has not yet been run.
   *
   * @return the PageRank vector, or null if PageRank has not yet been run
   */
  public AtomicDoubleArray getPageRankVector() {
    return prVector;
  }

  /**
   * A PageRank worker thread that distributes PageRank mass for a partition of nodes in a
   * particular iteration.
   */
  private class PageRankWorker extends Thread {
    final int id;
    final AtomicDoubleArray nextPR;
    final LongArrayList nodes;
    final CountDownLatch latch;
    final double mass;

    /**
     * Creates a PageRank worker thread.
     *
     * @param id      partition id
     * @param nextPR  the PageRank vector to modify
     * @param nodes   the nodes this thread is responsible for
     * @param latch   countdown latch to synchronize all worker threads for an iteration
     * @param mass    PageRank mass to pass along (from dangling nodes and from damping)
     */
    public PageRankWorker(int id, AtomicDoubleArray nextPR, LongArrayList nodes,
                          CountDownLatch latch, double mass) {
      this.id = id;
      this.nextPR = nextPR;
      this.nodes = nodes;
      this.latch = latch;
      this.mass = mass;
    }

    @Override
    public void run() {
      // Each thread runs on its own partition of the nodes, distributing PageRank mass for the
      // nodes that were assigned to it.
      final LongIterator iter = nodes.iterator();
      while (iter.hasNext()) {
        long v = iter.nextLong();
        //v as the index
        int outDegree = graph.getNode((int)v).getOutDegree();
        if (outDegree > 0) {
          double outWeight = dampingFactor * prVector.get((int) v) / outDegree;
          
          Iterator<Edge> edges = graph.getNode((int)v).leavingEdges().iterator();
          while (edges.hasNext()) {
        	  Edge tmp = edges.next();
            int nbr = (int) tmp.getNode1().getIndex();  //.nextLong();
            // Note that getAndAdd is implemented as an atomic operation with CAS, so
            // it's fine to have concurrent accesses to the PageRank vector.
            nextPR.getAndAdd(nbr, outWeight);
          }
        }

        nextPR.getAndAdd((int) v, mass);
      }

      latch.countDown();
    }
  }
}
