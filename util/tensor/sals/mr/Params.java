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

/**
 * Parameters of algorithms
 * <P>
 * @author Kijung
 */
public class Params {

	////////////////////////////////////
	// public fields
	////////////////////////////////////
	
	public final static String P_TRAINING = "training"; // training data file path in hdfs
	public final static String P_OUTPUT = "output"; // output directory path in hdfs
	public final static String P_M = "M"; // number of reducers
	public final static String P_TOUT = "Tout"; // number of outer iterations
	public final static String P_TIN = "Tin"; // number of inner iterations
	public final static String P_N = "N"; // dimension of the input tensor
	public final static String P_K = "K"; // rank of the input tensor
	public final static String P_C = "C"; // number of parameters updated at a time
	public final static String P_LAMBDA = "lambda"; // regularization parameter
	public final static String P_LAMBDA_BIAS = "lambda_for_biases"; // regularization parameter
	public final static String P_REGULARIZATION = "regularization"; // regularization method (1: L1-regularization, 2: L2-regularization)
	public final static String P_USE_WEIGHT = "use_weight"; // whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	public final static String P_NONNEGATIVE = "non_negative"; // whether to use nonnegativity constraint
	public final static String P_MODELENGTH = "I"; // I_{n}: length of the nth mode 
	public final static String P_INDEX = "i"; // column index of nth mode index
	public final static String P_INDEX_VALUE = "iv"; // column index of entry value
	public final static String P_TEST = "test"; // test data file path
	public final static String P_QUERY = "query"; // query data file path
	public final static String P_DELIM = "delim"; // delimiter of the training data file path
	public final static String P_WAITING = "waiting"; // time in millisecond to wait for other machines to write their parameters
	public final static String P_CARDINALITY = "cardinality"; // path of the file to save the number of nonzero elements in each fiber
	public final static String P_INDEXMAP= "indexmap"; //path of the file to save the result of greedy assignment
	public final static String P_ESTIMATE= "estimate"; // path of the file to save the estimated values for query entries
	public final static String P_PERFORMANCE_TEMP= "performance temp"; // name of the directory to save performance statistics of each reducer
	public final static String P_PERFORMANCE= "performance"; // path of the file to save aggregated performance statistics
	public final static String P_FACTORMATRICES= "factor_matrices"; // path of the file to save factor matrices
	public final static String P_AVERAGE_PATH= "average_path"; // path of the file to save the estimated values for query entries\
	public final static String P_AVERAGE = "average"; // the average of the entries of training data
	public final static String P_USE_BIAS = "use_bias"; // whether to use bias terms
	public final static String P_BIASES= "biases"; // path of the file to save bias terms
	public final static String P_SEED = "seed"; // random seed for initializing factor matrices
	public final static String P_ALPHA = "alpha"; // rank of the input tensor
	public final static String P_MEMORY = "mapred.child.java.opts"; // amount of heap space allocated to each reducer
	
}
