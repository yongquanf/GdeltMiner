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
 * Common functions related to measure performance
 * <P>
 * @author Kijung
 */
public class Performance {
	
	/**
	 * Compute RMSE in a parallel way
	 * @param tensor	tensor
	 * @param params	factor matrices	(n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @param N	dimension
	 * @param K	rank
	 * @param M	# of machine 
	 * @return	RMSE
	 */
	public static double computeRMSE(final Tensor tensor, final float[][][] params, final int N, final int K, final int M){
		
		final double[] loss = new double[1];
		
		new MultiThread<Object>(){

			@Override
			public Object runJob(int m, int threadIndex) {

				double innerLoss = 0;

				
				// start and end index of X entries assigned to machine m
				int[] indicies = blockIndex(tensor.omega, M, m);
				int startIdx = indicies[0];
				int endIdx = indicies[1];

				for(int elemIdx =startIdx; elemIdx <= endIdx; elemIdx++){
						
					//estimated value
					float predict = 0;
					for(int k=0; k<K; k++){
						float product = 1;
						for(int n=0; n<N; n++){
							product *= params[n][tensor.indices[n][elemIdx]][k];
						}
						predict += product;
					}
						innerLoss += Math.pow((predict - tensor.values[elemIdx]), 2);
				}
				

				synchronized(loss){
					loss[0]+=innerLoss;
				}

				return null;
			} 

		}.run(M, MultiThread.createJobList(M));
		
		return Math.sqrt(loss[0]/tensor.omega);
	}
	
	/**
	 * Compute RMSE in a parallel way
	 * @param tensor	tensor
	 * @param params	factor matrices	(n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @param bias	bias terms	(n, i_{n}) -> b^{(n)}_{i_{n}} 
	 * @param mu	mu
	 * @param N	dimension
	 * @param K	rank
	 * @param M	# of machine 
	 * @return	RMSE
	 */
	public static double computeRMSE(final Tensor tensor, final float[][][] params, final float[][] bias, final float mu, final int N, final int K, final int M){
		
		final double[] loss = new double[1];
		
		new MultiThread<Object>(){

			@Override
			public Object runJob(int m, int threadIndex) {

				double innerLoss = 0;
			
				// start and end index of X entries assigned to machine m
				int[] indicies = blockIndex(tensor.omega, M, m);
				int startIdx = indicies[0];
				int endIdx = indicies[1];

				for(int elemIdx =startIdx; elemIdx <= endIdx; elemIdx++){
					
					//estimated value
					float predict = mu;
					for(int n=0; n<N; n++){
						predict += bias[n][tensor.indices[n][elemIdx]];
					}
						
					for(int k=0; k<K; k++){
						float product = 1;
						for(int n=0; n<N; n++){
							product *= params[n][tensor.indices[n][elemIdx]][k];
						}
						predict += product;
					}
					innerLoss += Math.pow((predict - tensor.values[elemIdx]), 2);
				}

				synchronized(loss){
					loss[0]+=innerLoss;
				}

				return null;
			} 

		}.run(M, MultiThread.createJobList(M));
		
		return Math.sqrt(loss[0]/tensor.omega);
	}
	
	/**
	 * start index and end index when divide n items into m machines
	 * @param n	# of items
	 * @param m	# of machines (cores)
	 * @param i	index of the block  
	 * @return (start, end)
	 */
	private static int[] blockIndex(int n, int m, int i){
		int[] result = new int[2];
		result[0] = (int) Math.ceil((n + 0.0) * i / m);
		result[1] = (int) Math.ceil((n + 0.0) * (i+1) / m) - 1;
		return result;
	}
	
}
