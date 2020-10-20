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

import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop version of Bias-SALS
 * <P>
 * @author Kijung
 */
public class Bias_SALS {
	
	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * main function to run Bias-ALS
	 * @param args	[training] [output] [M] [Tout] [Tin] [N] [K] [C] [useWeight] [lambda] [lambdaForBiases] [I1] [I2] ... [IN] [memory] [test] [query]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SALS(true), args);
		System.exit(exitCode); 
	}
}