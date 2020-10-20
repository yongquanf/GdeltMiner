package EventGraph.IncrementalLearning;

import java.io.BufferedReader;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import EventGraph.IncrementalLearning.NC.GetOpt;
import EventGraph.IncrementalLearning.NC.NCClient;
import EventGraph.IncrementalLearning.NC.RemoteState;


/**
 * todo, 
 * S1: each node inits a local coordinate;
 * S2: each node performs random walks, samples neighbors
 * S3: each node computes personalized pagerank to this neighbor
 * S4: each node uses the sample personalized pagerank distance to 
 * update its own coordinate
 * 
 * 
 * update the representation learning for dynamic nodes, edges
 * method: Vivaldi-hyperbolic, decentralized maintenance
 * @author eric
 *
 */
public class IncrementalRepresentationLearning {

	/**
	 * init
	 * @param args
	 */
	public void init(String[] args) {
		
		long seed = new Date().getTime();
		RemoteState.MIN_SAMPLE_SIZE = 0;
		RemoteState.MAX_SAMPLE_SIZE = 1;
		NCClient.GRAVITY_DIAMETER = 0;
		NCClient.KEEP_STATS = true;

		// Note: remember that too large of an axis will cause some pairs of
		// nodes to be
		// too far apart from each other. I.e., the RTT will exceed the HUGE
		// value

	
		GetOpt g = new GetOpt( args,
				"n:l:dA:bx:zO:r:I:Zs:L:S:R:");
		
	}
	
	/**
	 * run
	 * @param nodeCount
	 * @param rttReader
	 * @param nodes
	 * @param stable
	 * @param symmetric
	 * @param SymRepeats
	 */
	public void run(int nodeCount,BufferedReader rttReader, List<Node> nodes,
			boolean stable, boolean symmetric,int SymRepeats) {
		
		
		boolean can_add = true;


		try {
			String sampleLine = rttReader.readLine();
			System.err.println(sampleLine);
			int counter = 0;

			while (sampleLine != null) {
				// reads in timestamp in ms and raw rtt
				StringTokenizer sampleTokenizer = new StringTokenizer(
						sampleLine);
				long curr_time = Long.parseLong((String) (sampleTokenizer
						.nextElement()));
				int from = Integer.parseInt((String) (sampleTokenizer
						.nextElement()));
				int to = Integer.parseInt((String) (sampleTokenizer
						.nextElement()));
				double rawRTT = Double.parseDouble((String) (sampleTokenizer
						.nextElement()));

				Node src = nodes.get(from);
				Node dst = nodes.get(to);

				// update RTT
				// rttMedian[from][to]=(float)rawRTT;
				// rttMedian[to][from]= rttMedian[from][to];
				// symmetric update
				int count = 0;

				if (stable) {
					if (symmetric) {
						while (count < SymRepeats) {
							// System.out.println("$: symmetric and stable process"+count);
							src.nc.processSample(dst.id, dst.nc.sys_coord,
									dst.nc.getSystemError(), rawRTT, dst.nc
											.getAge(curr_time), curr_time,
									can_add);
							dst.nc.processSample(src.id, src.nc.sys_coord,
									src.nc.getSystemError(), rawRTT, src.nc
											.getAge(curr_time), curr_time,
									can_add);
							count++;
						}
					} else {
						src.nc.processSample(dst.id, dst.nc.sys_coord, dst.nc
								.getSystemError(), rawRTT, dst.nc
								.getAge(curr_time), curr_time, can_add);
					}
				} else {
					if (symmetric) {
						while (count < SymRepeats) {
							src.nc.processSample_noStable(dst.id,
									dst.nc.sys_coord, dst.nc.getSystemError(),
									rawRTT, dst.nc.getAge(curr_time),
									curr_time, can_add);
							dst.nc.processSample_noStable(src.id,
									src.nc.sys_coord, src.nc.getSystemError(),
									rawRTT, src.nc.getAge(curr_time),
									curr_time, can_add);
							count++;
						}
					} else {
						src.nc.processSample_noStable(dst.id, dst.nc.sys_coord,
								dst.nc.getSystemError(), rawRTT, dst.nc
										.getAge(curr_time), curr_time, can_add);
					}
				}

				sampleLine = rttReader.readLine();
				
			}
		} catch (Exception ex) {
			System.err.println("Problem parsing " + ex);
			System.exit(-1);
		}
	}
	
	class Node{
		final public int id;
		final public NCClient<Integer> nc;
		int coveredNodes = 0;

		public Node(int _id, NCClient<Integer> _nc) {
			id = _id;
			nc = _nc;
		}
	}
}
