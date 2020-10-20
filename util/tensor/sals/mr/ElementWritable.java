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

import org.apache.hadoop.io.Writable;

/**
 * Value type to save tensor entries
 * <P>
 * @author Kijung
 */
public class ElementWritable implements Writable{
	
	////////////////////////////////////
	// public fields
	////////////////////////////////////
	
	public static int N; // dimension
	public int[] index; // indices of the entry n -> i_{n}
	public float value; // entry value (x_{i_{1}i_{2}...i_{N}})
	public boolean isTraining; // true: this entry belongs to training data, false: this entry belong to test dat or query data

	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	public ElementWritable() {};
	
	/**
	 * set fields
	 * @param index indices of the entry (n -> i_{n})
	 * @param value value of the entry (x_{i_{1}i_{2}...i_{N}})
	 * @param isTraining // true: this entry belongs to training data, false: this entry belong to test dat or query data
	 */
	public void set(int[] index, float value, boolean isTraining){
		this.index = index;
		this.value = value;
		this.isTraining = isTraining;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		index = new int[N];
		for(int i=0; i<N; i++){
			index[i] = in.readInt();
		}
		value = in.readFloat();
		isTraining = in.readBoolean();
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		for(int i=0; i<N; i++){
			out.writeInt(index[i]);
		}
		out.writeFloat(value);
		out.writeBoolean(isTraining);
	}
	
}
