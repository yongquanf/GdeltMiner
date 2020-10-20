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


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Key type to save pairs
 * <P>
 * @author Kijung
 */
public class PairWritable implements WritableComparable<PairWritable>{

	////////////////////////////////////
	// public fields
	////////////////////////////////////
	
	public int left;
	public int right;
	
	////////////////////////////////////
	// private fields
	////////////////////////////////////
	
	private static final int PRIME = 1000003;
	
	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	public PairWritable(){}
	
	/**
	 * set fields
	 * @param left left element
	 * @param right	right element
	 */
	public void set(int left, int right){
		this.left = left;
		this.right = right;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		left = in.readInt();
		right = in.readInt();
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(left);
		out.writeInt(right);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(PairWritable target) {
		int cmp = left - target.left;
		if(cmp != 0)
			return cmp;
		else
			return right - target.right;
	}
	
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return left * PRIME + right;
    }
	
}
