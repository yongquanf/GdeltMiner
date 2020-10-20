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

import java.util.Random;

/**
 * Single machine version of CDTF
 * <P>
 * @author Kijung
 */
public class CDTF {

	////////////////////////////////////
	//public methods
	////////////////////////////////////

	/**
	 * main function to run CDTF
	 * @param args	[training] [output] [M] [Tout] [Tin] [N] [K] [regularization] [useWeight] [nonNegative] [lambda] [I1] [I2] ... [IN] [test] [query]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		run(args, false);
	}

	/**
	 * run CDTF
	 * @param args
	 * @param useBias	whether to use bias
	 * @throws Exception
	 */
	public static void run(String[] args, boolean useBias) throws Exception{

		boolean inputError = true;

		try {

			/*
			 * parameter check
			 */
			System.out.println("========================");
			System.out.println("Start parameter check...");
			System.out.println("========================");

			String training = args[0];
			System.out.println("-training: "+training);
			String outputDir = args[1];
			System.out.println("-output: "+outputDir);
			int M = Integer.valueOf(args[2]);
			System.out.println("-M: "+M);
			int Tout = Integer.valueOf(args[3]);
			System.out.println("-Tout: "+Tout);
			int Tin = Integer.valueOf(args[4]);
			System.out.println("-Tin: "+Tin);
			int N = Integer.valueOf(args[5]);
			System.out.println("-N: "+N);
			int K = Integer.valueOf(args[6]);
			System.out.println("-K: "+K);
			int regularization = Integer.valueOf(args[7]);
			System.out.println("-regularization "+regularization);
			boolean useWeight = Integer.valueOf(args[8]) > 0;
			System.out.println("-useWeight: "+useWeight);
			boolean nonNegative = Integer.valueOf(args[9]) > 0;
			System.out.println("-nonNegative: "+nonNegative);
            float lambda = Float.valueOf(args[10].trim());
            System.out.println("-lambda: "+lambda);
            float lambdaForBiases = 0;
            int base = 11;
            if(useBias) {
                lambdaForBiases = Float.valueOf(args[base].trim());
                System.out.println("-lambdaBias: "+lambdaForBiases);
                base++;
            }
			int[] modeSizes = new int[N];
			for(int n=0; n<N; n++){
				modeSizes[n] = Integer.valueOf(args[base+n]);
				System.out.println("-I"+(n+1)+": "+modeSizes[n]);
			}

			String test = null;
			if(args.length > base+N ){
				test = args[base+N].trim();
				System.out.println("-test: "+test);
			}
			String query = null;
			if(args.length > base+N+1 ){
				query = args[base+N+1].trim();
				System.out.println("-query: "+query);
			}

			inputError = false;

			double[][] result = null;

			/*
			 * greedy row assignment
			 */
			int[] modesIdx = ArrayMethods.createSequnce(N);

			System.out.println("==============================");
			System.out.println("Start greedy row assignment...");
			System.out.println("==============================");

			int[][] permutedIdx = GreedyRowAssignment.run(training, N, modesIdx, modeSizes, M);

			Tensor testTensor = null;
			if(test!=null){
				testTensor = TensorMethods.importSparseTensor(test, ",", modeSizes, modesIdx, N, permutedIdx);
			}

			String name = (useBias) ? "Bias-CDTF" : "CDTF";

			System.out.println("=============");
			System.out.println("Start "+name+"...");
			System.out.println("=============");

			CDTF method = new CDTF();

			Tensor trainingTensor = TensorMethods.importSparseTensor(training, ",", modeSizes, modesIdx, N, permutedIdx);
			if(test!=null){
				method.setTest(testTensor);
			}

			result = method.run(trainingTensor, K, Tout, Tin, M, lambda, lambdaForBiases, regularization, useWeight, nonNegative, useBias, true);

			System.out.println("=======================");
			System.out.println("Start writing output...");
			System.out.println("=======================");

			Output.writePerformance(outputDir, result, Tout);
			Output.writeFactorMatrices(outputDir, method.params, permutedIdx);
			if(useBias){
				Output.writeBiases(outputDir, method.bias, permutedIdx);
				Output.writeMU(outputDir, method.mu);
			}
			if(query!=null){
				Tensor queryTensor = TensorMethods.importSparseTensor(query, ",", modeSizes, modesIdx, 0, permutedIdx);
				if(useBias)
					Output.calculateEstimate(queryTensor, method.mu, method.bias, method.params, N, K);
				else
					Output.calculateEstimate(queryTensor, method.params, N, K);
				Output.writeEstimate(outputDir, queryTensor, permutedIdx, N);
			}

			System.out.println("===========");
			System.out.println("Complete!!!");
			System.out.println("===========");

		} catch(Exception e){
			if(inputError){
				String fileName = (useBias) ? "run_single_bias_cdtf.sh" : "run_single_cdtf.sh";
				if(useBias)
                    System.err.println("Usage: " + fileName + " [training] [output] [M] [Tout] [Tin] [N] [K] [regularization] [useWeight] [nonNegative] [lambda] [lambdaBias] [I1] [I2] ... [IN] [test] [query]");
                else
                    System.err.println("Usage: " + fileName + " [training] [output] [M] [Tout] [Tin] [N] [K] [regularization] [useWeight] [nonNegative] [lambda] [I1] [I2] ... [IN] [lambda] [test] [query]");
                e.printStackTrace();
			}
			else {
				throw e;
			}
		}
	}

	////////////////////////////////////
	//private fields
	////////////////////////////////////

	private static float epsilon = 0.000000000001f;

	/**
	 * test data
	 */
	private Tensor test;

	/**
	 * factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 */
	private float[][][] params;

	/**
	 * bias terms (n, i_{n}) -> b^{(n)}_{i_{n}}
	 */
	private float[][] bias;

	/**
	 * the average of the training entries
	 */
	private float mu;

	////////////////////////////////////
	//private methods
	////////////////////////////////////

	/**
	 * set test data
	 * @param test	test data
	 */
	private void setTest(Tensor test){
		this.test = test;
	}

	/**
	 *
	 * run CDTF
	 *
	 * @param training	training data
	 * @param K	rank
	 * @param Tout	number of outer iterations
	 * @param Tin	number of inner iterations
	 * @param M	number of cores
	 * @param lambda	regularization parameter for factor matrices
     * @param lambdaForBias	regularization parameter for bias terms
	 * @param regularization	regularization method (1: L1-regularization, 2: L2-regularization)
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param nonNegative	whether to use nonnegativity constraint
	 * @param useBias	whether to use bias terms
	 * @param printLog	whether to print logs
	 * @return statistics per iteration [(iteration, elapsed time, trainingRMSE, testRMSE), ...]
	 */
	public double[][] run(Tensor training, final int K, int Tout, int Tin, final int M, final float lambda, final float lambdaForBias, final int regularization, final boolean useWeight, final boolean nonNegative, final boolean useBias, boolean printLog){

		Random random = new Random();

		if(printLog){
			System.out.println("iteration,elapsed_time,training_rmse,test_rmse");
		}

		final Tensor R =  training.copy(); // residual tensor

		final int nnzTraining = R.omega; // |\Omega| number of observable entries in training data
		boolean useTest = test!=null;

		final int N = R.N; // dimension
		final int[] modeLength = R.modeLengths; // n -> I_{n}

		final int[][] nnzFiber = TensorMethods.cardinality(R); // (n, i_{n}) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber

		/*
		 * distribute the rows of factor matrices across machines
		 */
		final int[][][] division = new int[M][N][]; // (m, n) -> {}_{m}\Omega^{(n)} (list of R entries used to update the factor matrix A^{(n)})
		final int[][] divisionCount = new int[M][N]; // (m, n) -> |{}_{m}\Omega^{(n)}|
		{
			// calculate the number of entries in each division
			for(int elemIdx=0; elemIdx<R.omega; elemIdx++){
				for(int n=0; n<N; n++){
					int bIndex = Math.min(R.indices[n][elemIdx]*M/modeLength[n], M-1);
					divisionCount[bIndex][n]++;
				}
			}

			//initialize each division
			for(int m=0; m<M; m++){
				for(int n=0; n<N; n++){
					division[m][n] = new int[divisionCount[m][n]];
					divisionCount[m][n]=0;
				}
			}

			//assign entries in each division
			for(int elemIdx=0; elemIdx<R.omega; elemIdx++){
				for(int n=0; n<N; n++){
					int bIndex = Math.min(R.indices[n][elemIdx]*M/modeLength[n], M-1);
					division[bIndex][n][divisionCount[bIndex][n]++]=elemIdx;
				}
			}
		}

		if(useBias){
			mu = training.mu;

			//initialize R
			new MultiThread<Object>(){

				@Override
				public Object runJob(int m, int threadIndex) {

					int[] indicies = blockIndex(nnzTraining, M, m);
					int rowStart = indicies[0];
					int rowEnd = indicies[1];

					for(int elemIdx =rowStart; elemIdx <= rowEnd; elemIdx++){
						R.values[elemIdx] -= mu;
					}

					return null;
				}

			}.run(M, MultiThread.createJobList(M));

			bias = new float[N][]; // bias terms (n, i_{n}) -> b^{(n)}_{i_{n}}
			for(int n=0; n<N; n++){
				bias[n] = new float[modeLength[n]]; //initialize to zero
			}
		}

		params = new float[N][][]; //factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
		for(int n=0; n<N; n++){
			params[n] = ArrayMethods.createUniformRandomMatrix(modeLength[n],K,(n==0) ? 0 : 1, random);
		}

		final float[][] updatedColumn = new float[N][]; // the columns of factor matrices currently updated
		for(int n=0; n<N; n++){
			updatedColumn[n] = new float[modeLength[n]];
		}

		double[][] result = new double[Tout][4]; //[(iteration, elapsed time, trainingRMSE, testRMSE), ...]
		long start = System.currentTimeMillis();

		for(int outIter=0; outIter<Tout; outIter++){

			for(int k =0; k <K; k++){

				final int _k = k;

				/*
				 * load the kth column of the factor matrices
				 */
				new MultiThread<Object>(){

					@Override
					public Object runJob(int blockIndex, int threadIndex) {

						for(int n=0; n<N; n++){
							int[] indicies = blockIndex(modeLength[n], M, blockIndex);
							int rowStart = indicies[0];
							int rowEnd = indicies[1];

							for(int row=rowStart; row<=rowEnd; row++){
								updatedColumn[n][row] = params[n][row][_k];
							}
						}

						return null;
					}

				}.run(M, MultiThread.createJobList(M));

				/*
				 * calculate \hat{R} for rank one factorization
				 */
				new MultiThread<Object>(){

					@Override
					public Object runJob(int blockIndex, int threadIndex) {

						int[] indicies = blockIndex(nnzTraining, M, blockIndex);
						int startIdx = indicies[0];
						int endIdx = indicies[1];

						updateR(R, updatedColumn, startIdx, endIdx, true);

						return null;
					}

				}.run(M, MultiThread.createJobList(M));


				for(int innerIter = 0; innerIter < Tin; innerIter++){

					/*
					 * rank one factorization
					 */
					for(int n=0; n<N; n++){

						final int _n = n;

						new MultiThread<Object>(){

							@Override
							public Object runJob(int m, int threadIndex) {

								int[] indicies = blockIndex(modeLength[_n], M, m);
								int startIdx = indicies[0];
								int endIdx = indicies[1];

								// update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
								updateFactors(R, division[m][_n], _n, updatedColumn, lambda, regularization, useWeight, nonNegative, startIdx, endIdx, nnzFiber[_n]);

								return null;
							}

						}.run(M, MultiThread.createJobList(M));
					}
				}

				/*
				 * update R entries and parameters
				 */
				new MultiThread<Object>(){

					@Override
					public Object runJob(int b, int threadIndex) {

						{
							int[] indicies = blockIndex(nnzTraining, M, b);
							int rowStart = indicies[0];
							int rowEnd = indicies[1];
							updateR(R, updatedColumn, rowStart, rowEnd, false);
						}

						for(int n=0; n<N; n++){
							int[] indicies = blockIndex(modeLength[n], M, b);
							int startIdx = indicies[0];
							int endIdx = indicies[1];
							for(int idx=startIdx; idx<=endIdx; idx++){
								params[n][idx][_k] = updatedColumn[n][idx];
							}
						}

						return null;
					}

				}.run(M, MultiThread.createJobList(M));

			}

			if(useBias){

				/*
				 * update biases
				 */
				for(int n=0; n<N; n++){

					final int _n = n;

					final float[] oldBias = bias[_n].clone(); //bias terms before update


					new MultiThread<Object>(){

						@Override
						public Object runJob(int m, int threadIndex) {

							int[] indicies = blockIndex(modeLength[_n], M, m);
							int startIdx = indicies[0];
							int endIdx = indicies[1];

							// update {}_{m}b^{(n)}{i_{n}}, the parameters in b^{(n)} assigned to machine m
							updateBiases(R, division[m][_n], _n, oldBias, bias[_n], lambdaForBias, regularization, useWeight, nonNegative, startIdx, endIdx, nnzFiber[_n]);

							return null;
						}

					}.run(M, MultiThread.createJobList(M));


					/*
					 * update R entries and parameters
					 */
					new MultiThread<Object>(){

						@Override
						public Object runJob(int b, int threadIndex) {

							{
								int[] indicies = blockIndex(nnzTraining, M, b);
								int rowStart = indicies[0];
								int rowEnd = indicies[1];
								updateR(R, _n, bias[_n], oldBias, rowStart, rowEnd);
							}

							return null;
						}

					}.run(M, MultiThread.createJobList(M));
				}
			}

			final double[] loss = new double[1];

			/*
			 * compute training RMSE
			 */
			new MultiThread<Object>(){

				@Override
				public Object runJob(int b, int threadIndex) {

					double innerLoss = 0;

					{
						int[] indicies = blockIndex(nnzTraining, M, b);
						int rowStart = indicies[0];
						int rowEnd = indicies[1];

						for(int elemIdx =rowStart; elemIdx <= rowEnd; elemIdx++){
							innerLoss += R.values[elemIdx] * R.values[elemIdx];
						}
					}

					synchronized(loss){
						loss[0]+=innerLoss;
					}

					return null;
				}

			}.run(M, MultiThread.createJobList(M));
			double trainingRMSE = Math.sqrt(loss[0]/nnzTraining);
			double testRMSE = 0;
			if(useTest){
				testRMSE = useBias ? Performance.computeRMSE(test, params, bias, mu, N, K, M) : Performance.computeRMSE(test, params, N, K, M);
			}

			long elapsedTime = System.currentTimeMillis()-start;

			if(printLog){
				System.out.printf("%d,%d,%f,%f\n",(outIter+1), elapsedTime, trainingRMSE, testRMSE);
			}

			result[outIter] = new double[]{(outIter+1), elapsedTime, trainingRMSE, testRMSE};
		}

		return result;

	}

	/**
	 * update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
	 * @param R	residual tensor
	 * @param Rindex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	updated mode
	 * @param updatedColumn	n -> a^{(n)}{i_{n}*}, the columns of fator matrices currently updated
	 * @param lambda	regularization parameter
	 * @param regularization	regularization method (1: L1-regularization, 2: L2-regularization)
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param nonNegative	whether to use nonnegativity constraint
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	i_{n} -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	public void updateFactors(Tensor R, int[] Rindex, int n, float[][] updatedColumn, float lambda, int regularization, boolean useWeight, boolean nonNegative, int firstRow, int lastRow, int[] nnzFiber){

		int dimension = updatedColumn.length;
		int numberOfRows = lastRow - firstRow+1;
		float[] numerators = new float[numberOfRows];
		float[] denominators = new float[numberOfRows];

		for(int idx=0; idx<Rindex.length; idx++){

			int elemIdx = Rindex[idx];
			float numerator = 1;
			float denominator = 1;
			for(int i=0; i<dimension; i++){
				if(i!=n)
					numerator *= updatedColumn[i][R.indices[i][elemIdx]];
			}
			denominator = numerator * numerator;
			numerator *= R.values[elemIdx];
			int resultIndex = R.indices[n][elemIdx]-firstRow;
			numerators[resultIndex] += numerator;
			denominators[resultIndex] += denominator;
		}

		if(regularization == 1) {
			for (int i = 0; i < numberOfRows; i++) {
				int idx = i + firstRow;
				float g = -2 * numerators[i];
				float d = 2 * denominators[i];

				if (denominators[i] != 0) {
					float result = 0;
					float weightedLambda = lambda * (useWeight ? nnzFiber[idx] : 1);
					if(g > weightedLambda) {
						result = (weightedLambda - g)/d;
					}
					else if (g < - weightedLambda) {
						result = - (weightedLambda + g)/d;
					}

					if (result > -epsilon && result < epsilon) { // to prevent underflow
						result = 0;
					}

					if (nonNegative) {
						result = Math.max(result, 0);
					}

					updatedColumn[n][idx] = result;
				}
			}
		}
		else if(regularization == 2) {
			for (int i = 0; i < numberOfRows; i++) {
				int idx = i + firstRow;
				denominators[i] += lambda * (useWeight ? nnzFiber[idx] : 1);
				if (denominators[i] != 0) {
					float result = numerators[i] / denominators[i];
					if (result > -epsilon && result < epsilon) { // to prevent underflow
						result = 0;
					}
					if (nonNegative) {
						result = Math.max(result, 0);
					}
					updatedColumn[n][idx] = result;
				}
			}
		}

	}

	/**
	 * update {}_{m}b^{(n)}{i_{n}}, the bias terms of b^{(n)} assigned to machine m
	 * @param R	residual tensor
	 * @param Rindex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	mode
	 * @param oldBias	b^{(n)} bias terms before update
	 * @param updatedBias	b^{(n)} bias terms updated
	 * @param lambda	regularization parameter
	 * @param regularization	regularization method (1: L1-regularization, 2: L2-regularization)
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param nonNegative	whether to use nonnegativity constraint
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	i_{n} -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	public void updateBiases(Tensor R, int[] Rindex, int n, float[] oldBias, float[] updatedBias, float lambda, int regularization, boolean useWeight, boolean nonNegative, int firstRow, int lastRow, int[] nnzFiber){

		int numberOfRows = lastRow - firstRow+1;
		float[] numerators = new float[numberOfRows];
		float[] denominators = new float[numberOfRows];

		for(int idx=0; idx<Rindex.length; idx++){
			int elemIdx = Rindex[idx];
			int rowIndex = R.indices[n][elemIdx];
			int resultIndex = rowIndex-firstRow;
			numerators[resultIndex] += R.values[elemIdx] + oldBias[rowIndex];
			denominators[resultIndex] += 1;
		}


		if(regularization == 1) {
			for (int i = 0; i < numberOfRows; i++) {
				int idx = i + firstRow;
				float g = -2 * numerators[i];
				float d = 2 * denominators[i];

				if (denominators[i] != 0) {
					float result = 0;
					float weightedLambda = lambda * (useWeight ? nnzFiber[idx] : 1);
					if(g > weightedLambda) {
						result = (weightedLambda - g)/d;
					}
					else if (g < - weightedLambda) {
						result = - (weightedLambda + g)/d;
					}

					if (result > -epsilon && result < epsilon) { // to prevent underflow
						result = 0;
					}
					if (nonNegative) {
						result = Math.max(result, 0);
					}
					updatedBias[idx] = result;
				}
			}
		}
		else if(regularization == 2) {
			for (int i = 0; i < numberOfRows; i++) {
				int idx = i + firstRow;
				denominators[i] += lambda * (useWeight ? nnzFiber[idx] : 1);
				if (denominators[i] != 0) {
					float result = numerators[i] / denominators[i];
					if (result > -epsilon && result < epsilon) { // to prevent underflow
						result = 0;
					}
					if (nonNegative) {
						result = Math.max(result, 0);
					}
					updatedBias[idx] = result;
				}
			}
		}

	}

	/**
	 * update R entries from startIdx to endIdx
	 * @param R	residual tensor
	 * @param updatedColumn
	 * @param startIdx	index of the first R entry updated
	 * @param endIdx	index of the last R entry updated
	 * @param add	true: update R / false: create \hat{R}
	 */
	private void updateR(Tensor R, float[][] updatedColumn, int startIdx, int endIdx, boolean add) {
		if(add){
			for(int idx = startIdx; idx <= endIdx ; idx++){
				float product=1;
				for(int n=0; n<updatedColumn.length; n++){
					product = product * updatedColumn[n][R.indices[n][idx]];
				}
				R.values[idx] = R.values[idx] + product;
			}
		}
		else{
			for(int idx = startIdx; idx <= endIdx ; idx++){
				float product=1;
				for(int n=0; n<updatedColumn.length; n++){
					product = product * updatedColumn[n][R.indices[n][idx]];
				}
				R.values[idx] = R.values[idx] - product;
			}
		}
	}

	/**
	 * update R entries from startIdx to endIdx
	 * @param R	residual tensor
	 * @param n mode
	 * @param updatedBias i_{n} -> b^{(n)}_{i_{n}} updated bias terms
	 * @param oldBias i_{n} -> (b^{(n)}_{i_{n}})^{old} bias terms before update
	 * @param startIdx	index of the first R entry updated
	 * @param endIdx	index of the last R entry updated
	 */
	private void updateR(Tensor R, int n, float[] updatedBias, float[] oldBias, int startIdx, int endIdx) {
		for(int idx = startIdx; idx <= endIdx ; idx++){
			int rowIdx = R.indices[n][idx];
			R.values[idx] = R.values[idx] + oldBias[rowIdx] - updatedBias[rowIdx];
		}

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


