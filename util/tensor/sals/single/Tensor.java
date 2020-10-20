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

package util.tensor.sals.single;

/**
 * Data structure to save tensor data
 * <P>
 * @author Kijung
 */
public class Tensor {
	
	////////////////////////////////////
	//public fields
	////////////////////////////////////
	
	public int N; //dimension
	public int[] modeLengths; // mode length of each mode (n -> I_{n})
	public int omega; // number of observable entries
	public int[][] indices; // (n, i) -> nth mode index of ith entry of the tensor
	public float[] values; // i -> entry value of ith entry of the tensor
	public float mu; // the average of the entries of the tensor
	
	
	////////////////////////////////////
	//public methods
	////////////////////////////////////
	/**
	 * 
	 * @param N	//dimension
	 * @param modeLengths	// mode length of each mode (n -> I_{n})
	 * @param omega	// number of observable entries
	 * @param indices	(i, n) -> nth mode index of ith entry of the tensor
	 * @param values	// i -> entry value of ith entry of the tensor
	 */
	public Tensor(int N, int[] modeLengths, int omega, int[][] indices, float[] values, float sum){
		this.N = N;
		this.modeLengths = modeLengths;
		this.omega = omega;
		this.indices = indices;
		this.values = values;
		this.mu = sum/omega;
	}
	
	/**
	 * copy
	 * @return	copied tensor
	 */
	public Tensor copy(){
		return new Tensor(this);
	}
	
	////////////////////////////////////
	//private methods
	////////////////////////////////////
	
	private Tensor(Tensor target){
		this.N = target.N;
		this.modeLengths = target.modeLengths.clone();
		this.omega = target.omega;
		this.indices = ArrayMethods.copy(target.indices);
		this.values = target.values.clone();
		this.mu = target.mu;
	}
	
}
