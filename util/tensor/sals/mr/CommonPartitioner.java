/* =================================================================================
 *
 * Fully Scalable Methods for Distributed Tensor Factorization
 * Authors: Kijung Shin (kijungs@cs.cmu.edu), Lee Sael, U Kang
 *
 * Version: 1.0
 * Date: April 10, 2016
 * Main Contact: Kijung Shin (kijungs@cs.cmu.edu)
 *
 * This software is free of charge under research purposes.
 * For commercial purposes, please contact the author.
 *
 * =================================================================================
 */

package util.tensor.sals.mr;


import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner class of CDTF, SALS, and ALS Hadoop version
 * <P>
 * @author Kijung
 */
public class CommonPartitioner extends Partitioner<TripleWritable, ElementWritable>{

	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * determine which machines each entry is assigned to
	 * 
	 * @param key <machine to assign this entry, order of the factor matrix that this entry is used to update>
	 * @param val <indices of the entry, value of the entry>
	 * @param numPartitions number of reducers
	 * @return machine to assign this entry
	 */
	public int getPartition(TripleWritable key, ElementWritable val, int numPartitions) {
		return key.left;
	}

}
