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


import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.commons.io.FileUtils;

import util.tensor.Jama.CholeskyDecomposition;
import util.tensor.Jama.MatrixJama;

/**
 * Single machine version of SALS
 * <P>
 * @author Kijung
 */
public class SALS {

	////////////////////////////////////
	//public methods
	////////////////////////////////////
	
	/**
	 * main function to run SALS
	 * @param args	[training] [output] [M] [Tout] [Tin] [N] [K] [C] [useWeight] [lambda] [I1] [I2] ... [IN] [test] [query]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		run(args, false);
	}
	
	/**
	 * run SALS
	 * @param args
	 * @param useBias	whether to use bias terms
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
			int C = Integer.valueOf(args[7]);
			System.out.println("-C: "+C);
			boolean useWeight = Integer.valueOf(args[8]) > 0;
			System.out.println("-useWeight: "+useWeight);
			float lambda = Float.valueOf(args[9]);
			System.out.println("-lambda: "+lambda);
            float lambdaForBiases = 0;
            int base = 10;
            if(useBias) {
                lambdaForBiases = Float.valueOf(args[base].trim());
                System.out.println("-lambdaForBiases: "+lambdaForBiases);
                base++;
            }
			int[] modeSizes = new int[N];
			for(int dim=0; dim<N; dim++){
				modeSizes[dim] = Integer.valueOf(args[base+dim]);
				System.out.println("-I"+(dim+1)+": "+modeSizes[dim]);
			}
			String test = null;
//			if(args.length > base+N){
//				test = args[base+N];
//				System.out.println("-test: "+test);
//			}
			String query = null;
//			if(args.length > base+N+1){
//				query = args[base+N+1];
//				System.out.println("-query: "+query);
//			}
			
			inputError = false;
							
			double[][] result = null;
			
			if(!FileUtils.getFile(outputDir).isDirectory()) {
				FileUtils.getFile(outputDir).mkdir();
			}
			
			
			/*
			 * greedy row assignment
			 */
			int[] modesIdx = ArrayMethods.createSequnce(N);
			
			System.out.println("==============================");
			System.out.println("Start greedy row assignment...");
			System.out.println("==============================");
			
			int[][] permutedIdx = GreedyRowAssignment.run(training, N, modesIdx, modeSizes, M);
			
			String name = (useBias) ? "Bias-SALS" : "SALS";
			
			/*
			 * run SALS
			 */
			System.out.println("=============");
			System.out.println("Start "+name+"...");
			System.out.println("=============");
			
			SALS method = new SALS();
			
			Tensor trainingTensor = TensorMethods.importSparseTensor(training, ",", modeSizes, modesIdx, N, permutedIdx);
			if(test!=null){
				Tensor testTensor = TensorMethods.importSparseTensor(test, ",", modeSizes, modesIdx, N, permutedIdx);
				method.setTest(testTensor);
			}
			result = method.run(trainingTensor, K, Tout, Tin, M, C, lambda, useWeight, useBias, true);
	
			/*
			 * write output
			 */
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
				String fileName = (useBias) ? "run_single_bias_sals.sh" : "run_single_sals.sh";
				System.err.println("Usage: " + fileName + " [training] [output] [M] [Tout] [Tin] [N] [K] [C] [lambda] [useWeight] [I1] [I2] ... [IN] [test] [query]");
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
	 * run SALS
	 * 
	 * @param training	training data
	 * @param K	rank
	 * @param Tout	number of outer iterations
	 * @param Tin	number of inner iterations
	 * @param M	number of cores
	 * @param C	number of columns updated at a time
	 * @param lambda	regularization parameter
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param useBias	whether to use bias terms
	 * @param printLog
	 * @return [(iteration, elapsed time, trainingRMSE, testRMSE), ...]
	 */
	public double[][] run(Tensor training, final int K, int Tout, 
			int Tin, final int M, final int C, final float lambda, 
			final boolean useWeight, final boolean useBias, boolean printLog){
		
		Random random = new Random();
		
		final Tensor R =  training.copy(); // residual tensor
		
		final int nnzTraining = R.omega; // |\Omega| number of observable entries in training data
		boolean useTest = test!=null;
		
		final int N = R.N; // dimension
		final int[] modeLengths = R.modeLengths; // n -> I_{n} 
		
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
					int bIndex = Math.min(R.indices[n][elemIdx]*M/modeLengths[n], M-1);
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
					int bIndex = Math.min(R.indices[n][elemIdx]*M/modeLengths[n], M-1);
					division[bIndex][n][divisionCount[bIndex][n]++]=elemIdx;
				}
			}
		}
		
		//sort each division so that entries used to update each fiber located contiguously
		for(int m=0; m<M; m++){
			for(int dim=0; dim<N; dim++){
				final int currentDim = dim;
				int[] arrayToSort = division[m][dim];
				Integer[] temp = new Integer[arrayToSort.length];
				for(int i=0; i<arrayToSort.length; i++){
					temp[i] = arrayToSort[i];
				}
				Arrays.sort(temp, new Comparator<Integer>(){
					public int compare(Integer elemIdx, Integer tElemIdx) {
						int modeIdx = R.indices[currentDim][elemIdx];
						int tModeIdx = R.indices[currentDim][tElemIdx];
						return tModeIdx - modeIdx;
					}
				});
				for(int i=0; i<arrayToSort.length; i++){
					arrayToSort[i] = temp[i];
				}
			}
		}
		
		if(useBias){
			mu = training.mu;
			
			//initialize R
			new MultiThread<Object>(){
				
				@Override
				public Object runJob(int b, int threadIndex) {
					
					int[] indicies = blockIndex(nnzTraining, M, b);
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
				bias[n] = new float[modeLengths[n]]; //initialize to zero
			}
		}
		
		params = new float[N][][]; //factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
		for(int dim=0; dim<N; dim++){
			params[dim] = ArrayMethods.createUniformRandomMatrix(modeLengths[dim], K, (dim==0) ? 0 : 1, random);
		}
		
		final float[][][] currentParams = new float[N][][]; // the columns of factor matrices currently updated (n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}
		for(int dim=0; dim<N; dim++){
			currentParams[dim] = new float[modeLengths[dim]][C];
		}

		double[][] result = new double[Tout][4]; //[(iteration, elapsed time, trainingRMSE, testRMSE), ...]
		long start = System.currentTimeMillis();
		
		for(int outIter=0; outIter<Tout; outIter++){
			
			final int[] permutedColumns = createRandomSequence(K, random);
			
			for(int splitIter =0; splitIter < Math.ceil((K+0.0)/C); splitIter++){
				
				final int cStart = C * splitIter;
				final int cEnd = Math.min(C * (splitIter +1) - 1, K - 1);
				final int cLength = cEnd - cStart + 1;
						
				/*
				 * load the k_{1}, ..., k_{c} columns of the factor matrices
				 */
				new MultiThread<Object>(){
					
					@Override
					public Object runJob(int blockIndex, int threadIndex) {
						
						for(int dim=0; dim<N; dim++){
							int[] indicies = blockIndex(modeLengths[dim], M, blockIndex);
							int rowStart = indicies[0];
							int rowEnd = indicies[1];
							
							for(int row=rowStart; row<=rowEnd; row++){
								for(int column = cStart; column <= cEnd; column++){
									currentParams[dim][row][column-cStart] = params[dim][row][permutedColumns[column]];		
								}
							}
						}
						
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
				
				/*
				 * calculate \hat_{R} for rank C factorization
				 */
				new MultiThread<Object>(){
						
					@Override
					public Object runJob(int blockIndex, int threadIndex) {
						
						int[] indicies = blockIndex(nnzTraining, M, blockIndex);
						int startIdx = indicies[0];
						int endIdx = indicies[1];
						
						updateR(R, currentParams, cLength, startIdx, endIdx, true);
						
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
				
				
				/*
				 * rank C factorization
				 */
				for(int innerIter = 0; innerIter < Tin; innerIter++){
					
					for(int dim=0; dim<N; dim++){
					
						final int currentDim = dim;
						
						new MultiThread<Object>(){
							
							@Override
							public Object runJob(int b, int threadIndex) {
					
								int[] indicies = blockIndex(modeLengths[currentDim], M, b);
								int startIdx = indicies[0];
								int endIdx = indicies[1];
								updateFactor(R, division[b][currentDim], currentDim, currentParams, cLength, lambda, useWeight, startIdx, endIdx, nnzFiber[currentDim]);
								
								return null;
							} 
						
						}.run(M, MultiThread.createJobList(M));
					}
				}
				
				/*
				 * update R entries
				 */	
				new MultiThread<Object>(){
						
					@Override
					public Object runJob(int b, int threadIndex) {
					
						int[] indicies = blockIndex(nnzTraining, M, b);
						int rowStart = indicies[0];
						int rowEnd = indicies[1];
						updateR(R, currentParams, cLength, rowStart, rowEnd, false);
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
				
				/*
				 * update paramters
				 */	
				new MultiThread<Object>(){
					
					@Override
					public Object runJob(int b, int threadIndex) {
						
						for(int dim=0; dim<N; dim++){
							int[] indicies = blockIndex(modeLengths[dim], M, b);
							int startIdx = indicies[0];
							int endIdx = indicies[1];
							for(int idx=startIdx; idx<=endIdx; idx++){
								for(int column = cStart; column <= cEnd; column++){
									params[dim][idx][permutedColumns[column]] = currentParams[dim][idx][column-cStart];
								}
							}
						}
						
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
			}
			
			if(useBias){
				//update biases
				for(int n=0; n<N; n++){
					
					final int _n = n;
					
					final float[] oldBias = bias[_n].clone(); //bias terms before update
					
					
					new MultiThread<Object>(){
								
						@Override
						public Object runJob(int m, int threadIndex) {
						
							int[] indicies = blockIndex(modeLengths[_n], M, m);
							int startIdx = indicies[0];
							int endIdx = indicies[1];
									
							updateBiases(R, division[m][_n], _n, oldBias, bias[_n], lambda, useWeight, startIdx, endIdx, nnzFiber[_n]);
								
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
	 * @param RIndex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	mode
	 * @param currentParams	(n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}, the columns of fator matrices currently updated
	 * @param cLength	min(C, the number of left rows)
	 * @param lambda	regularization parameter
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	(n) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	private static void updateFactor(Tensor R, int[] RIndex, int n, float[][][] currentParams, int cLength, float lambda, boolean useWeight, int firstRow, int lastRow, int[] nnzFiber){

		int dimension = currentParams.length;
		
		double[][] B = new double[cLength][cLength];
		double[][] c = new double[cLength][1]; 
		int[] indices = new int[dimension];
		int oldResultIndex = -1;
		
		for(int idx=0; idx<RIndex.length; idx++){

			int elemIdx = RIndex[idx];
			int resultIndex = R.indices[n][elemIdx];
			
			if(oldResultIndex>=0 && oldResultIndex!=resultIndex){
				
				//symmetrize
				for(int r1=0; r1<cLength; r1++){
					for(int r2=r1+1; r2<cLength; r2++){
						B[r2][r1] = B[r1][r2];
					}
				}
				
				//add identity matrix
				for(int r=0; r<cLength; r++){
					B[r][r] += lambda * (useWeight ? nnzFiber[oldResultIndex] : 1);
				}
				
				double[][] newParam = new CholeskyDecomposition(new MatrixJama(B)).solve(new MatrixJama(c)).getArray();
				for(int r=0; r<cLength; r++){
					float result = (float)newParam[r][0];
					if(result > -epsilon && result < epsilon){
						result = 0;
					}
					currentParams[n][oldResultIndex][r] = result;
				}
				
				//clear
				B = new double[cLength][cLength];
				c = new double[cLength][1]; 
			}
			
			oldResultIndex = resultIndex;
			
			// \prod_{l \neq n}a^{(l)}_{i_{l}k_{c}}
			double[] product = new double[cLength];
			for(int r=0; r<cLength; r++){
				product[r] = 1;
			}
			for(int dim=0; dim<dimension; dim++){
				indices[dim] = R.indices[dim][elemIdx];
			}
			
			for(int i=1; i<dimension; i++){
				int nextmode = (n+i)%dimension;
				for(int r=0; r<cLength; r++){
					product[r] *= currentParams[nextmode][indices[nextmode]][r];
				}
			}
			
			float value = R.values[elemIdx];
			for(int r1=0; r1<cLength; r1++){
				for(int r2=r1; r2<cLength; r2++){
					B[r1][r2] += product[r1] * product[r2];
				}
				c[r1][0] += product[r1] * value;
			}
			
		}
		
		if(oldResultIndex>=0){
			
			for(int r1=0; r1<cLength; r1++){
				for(int r2=r1+1; r2<cLength; r2++){
					B[r2][r1] = B[r1][r2];
				}
			}
			
			for(int r=0; r<cLength; r++){
				B[r][r] += lambda * (useWeight ? nnzFiber[oldResultIndex] : 1);
			}
			
			double[][] newParam = new CholeskyDecomposition(new MatrixJama(B)).solve(new MatrixJama(c)).getArray();
			for(int r=0; r<cLength; r++){
				float result = (float)newParam[r][0];
				if(result > -epsilon && result < epsilon){
					result = 0;
				}
				currentParams[n][oldResultIndex][r] = result;
			}
		}
	}
	
	/**
	 * update {}_{m}b^{(n)}{i_{n}}, the bias terms of b^{(n)} assigned to machine m
	 * @param R	residual tensor
	 * @param Rindex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	mode
	 * @param oldBias	b^{(n)} bias terms updated
	 * @param currentBias	b^{(n)} bias terms updated
	 * @param lambda	regularization parameter
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	(n) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	private void updateBiases(Tensor R, int[] Rindex, int n, float[] oldBias, float[] currentBias, float lambda, boolean useWeight, int firstRow, int lastRow, int[] nnzFiber){
		
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
		
		for(int i=0; i<numberOfRows; i++){
			int idx = i+firstRow;
			denominators[i] += lambda * (useWeight ? nnzFiber[idx] : 1);
			if(denominators[i]!=0){
				float result = numerators[i] / denominators[i];
				if(result > -epsilon && result < epsilon){ // to prevent underflow
					result = 0;
				}
				currentBias[idx] = result;
			}
		}
		
	}
	
	/**
	 * update R entries from startIdx to endIdx
	 * @param R	residual tensor
	 * @param n mode
	 * @param currentBias i_{n} -> b^{(n)}_{i_{n}} updated bias terms
	 * @param oldBias i_{n} -> (b^{(n)}_{i_{n}})^{old} bias terms before update
	 * @param startIdx	index of the first R entry updated
	 * @param endIdx	index of the first R entry updated
	 */
	private void updateR(Tensor R, int n, float[] currentBias, float[] oldBias, int startIdx, int endIdx) {
		for(int idx = startIdx; idx <= endIdx ; idx++){
			int rowIdx = R.indices[n][idx];
			R.values[idx] = R.values[idx] + oldBias[rowIdx] - currentBias[rowIdx];
		}
		
	}
	
	/**
	 * update R entries from startIdx to endIdx
	 * @param R	residual tensor
	 * @param currentParams
	 * @param startIdx	index of the first R entry updated
	 * @param endIdx	index of the first R entry updated
	 * @param add	true: update R / false: create \hat{R} 
	 */
	private double updateR(Tensor R, float[][][] currentParams, int columLength, int startIdx, int endIdx, boolean add) {
		double loss = 0;
		int dimension = currentParams.length;
		int[] indices = new int[dimension];
		if(add){
			for(int idx = startIdx; idx <= endIdx ; idx++){
				for(int dim=0; dim<dimension; dim++){
					indices[dim] = R.indices[dim][idx];
				}
				for(int columnIndex=0; columnIndex<columLength; columnIndex++){
					float product=1;
					for(int dim=0; dim<currentParams.length; dim++){
						product = product * currentParams[dim][indices[dim]][columnIndex];
					}
					R.values[idx] = R.values[idx] + product;
				}
			}
		}
		else{
			for(int idx = startIdx; idx <= endIdx ; idx++){
				for(int dim=0; dim<dimension; dim++){
					indices[dim] = R.indices[dim][idx];
				}
				for(int columnIndex=0; columnIndex<columLength; columnIndex++){
					float product=1;
					for(int dim=0; dim<currentParams.length; dim++){
						product = product * currentParams[dim][indices[dim]][columnIndex];
					}
					R.values[idx] = R.values[idx] - product;
				}
			}
		}
		return loss;
		
	}
	
	
	/**
	 * create randomly shuffled sequence (0 through n-1) 
	 * @param n	length of sequence
	 * @return	created sequence
	 */
	private static int[] createRandomSequence(int n, Random random){
		int[] result = new int[n];
		for(int i=0; i<n; i++){
			result[i] = i;
		}
		shuffle(result, random);
		return result;
	}
	
	/**
	 * shuffle given vector
	 * @param vec
	 */
	private static void shuffle(int[] vec, Random random){
		int n = vec.length;
	    for (int i = 0; i < n; i++){
	    	int randI = random.nextInt(n-i);
	    	int temp = vec[n-i-1];
	    	vec[n-i-1] = vec[randI];
	    	vec[randI] = temp;
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

