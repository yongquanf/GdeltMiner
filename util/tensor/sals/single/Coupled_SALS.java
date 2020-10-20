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


import util.tensor.Jama.CholeskyDecomposition;
import util.tensor.Jama.MatrixJama;

import java.util.*;

/**
 * Single machine version of Coupled SALS
 * <P>
 * @author Kijung
 */
public class Coupled_SALS {

	////////////////////////////////////
	//public methods
	////////////////////////////////////

    public static void main(String[] args) throws Exception{
        run(args);
    }
	
	/**
	 * run Coupled SALS
	 * @param args	[training] [coupled_tensor] [output] [M] [Tout] [Tin] [N1] [N2] [K] [C] [useWeight] [lambda] [I1] [I2] ... [IN1] [J1] [J2] ... [JN2] [test] [query]
	 * @throws Exception
	 */
	public static void run(String[] args) throws Exception{
		
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
			String coupled_tensor = args[1];
			System.out.println("-coupled_tensor: "+coupled_tensor);
			String outputDir = args[2];
			System.out.println("-output: "+outputDir);
			int M = Integer.valueOf(args[3]);
			System.out.println("-M: "+M);
			int Tout = Integer.valueOf(args[4]);
			System.out.println("-Tout: "+Tout);
			int Tin = Integer.valueOf(args[5]);
			System.out.println("-Tin: "+Tin);
			int N[] = new int[2];
			N[0] = Integer.valueOf(args[6]);
			System.out.println("-N1: "+N[0]);
			N[1] = Integer.valueOf(args[7]);
			System.out.println("-N2: "+N[1]);
			int K = Integer.valueOf(args[8]);
			System.out.println("-K: "+K);
			int C = Integer.valueOf(args[9]);
			System.out.println("-C: "+C);
			boolean useWeight = Integer.valueOf(args[10]) > 0;
			System.out.println("-useWeight: "+useWeight);
			float lambda = Float.valueOf(args[11]);
			System.out.println("-lambda: "+lambda);
			int[][] modeSizes = new int[2][];
			modeSizes[0] = new int[N[0]];
			for(int n=0; n<N[0]; n++){
				modeSizes[0][n] = Integer.valueOf(args[12+n]);
				System.out.println("-I"+(n+1)+": "+modeSizes[0][n]);
			}
			modeSizes[1] = new int[N[1]];
			for(int n=0; n<N[1]; n++){
				modeSizes[1][n] = Integer.valueOf(args[12+N[0]+n]);
				System.out.println("-J"+(n+1)+": "+modeSizes[1][n]);
			}
			String test = null;
			if(args.length > 12+N[0]+N[1] ){
				test = args[12+N[0]+N[1]].trim();
				System.out.println("-test: "+test);
			}
			String query = null;
			if(args.length > 13+N[0]+N[1] ){
				query = args[13+N[0]+N[1]].trim();
				System.out.println("-query: "+query);
			}

			inputError = false;
							
			double[][] result = null;
			
			/*
			 * greedy row assignment
			 */
			int[][] modesIdx = new int[][]{ArrayMethods.createSequnce(N[0]), ArrayMethods.createSequnce(N[1])};
			
			System.out.println("==============================");
			System.out.println("Start greedy row assignment...");
			System.out.println("==============================");

			int[][][] permutedIdx = new int[][][]{
					GreedyRowAssignment.run(training, N[0], modesIdx[0], modeSizes[0], M),
					GreedyRowAssignment.run(coupled_tensor, N[1], modesIdx[1], modeSizes[1], M)
			};
            permutedIdx[1][0] = permutedIdx[0][0];
			
			String name = "Coupled-SALS";
			
			/*
			 * run SALS
			 */
			System.out.println("=============");
			System.out.println("Start "+name+"...");
			System.out.println("=============");
			
			Coupled_SALS method = new Coupled_SALS();

			Tensor[] trainingTensor = new Tensor[]{
					TensorMethods.importSparseTensor(training, ",", modeSizes[0], modesIdx[0], N[0], permutedIdx[0]),
					TensorMethods.importSparseTensor(coupled_tensor, ",", modeSizes[1], modesIdx[1], N[1], permutedIdx[1])
			};
			if(test!=null){
				Tensor testTensor = TensorMethods.importSparseTensor(test, ",", modeSizes[0], modesIdx[0], N[0], permutedIdx[0]);
				method.setTest(testTensor);
			}
			result = method.run(trainingTensor, K, Tout, Tin, M, C, lambda, useWeight, true);
	
			/*
			 * write output
			 */
			System.out.println("=======================");
			System.out.println("Start writing output...");
			System.out.println("=======================");
			
			Output.writePerformance(outputDir, result, Tout);
			Output.writeFactorMatrices(outputDir, method.params[0], permutedIdx[0]);
			if(query!=null){
				Tensor queryTensor = TensorMethods.importSparseTensor(query, ",", modeSizes[0], modesIdx[0], 0, permutedIdx[0]);
				Output.calculateEstimate(queryTensor, method.params[0], N[0], K);
				Output.writeEstimate(outputDir, queryTensor, permutedIdx[0], N[0]);
			}
			
			System.out.println("===========");
			System.out.println("Complete!!!");
			System.out.println("===========");
		
		} catch(Exception e){
			if(inputError){
				String fileName = "run_single_coupled_sals.sh";
				System.err.println("Usage: " + fileName + " [training] [coupled_tensor] [output] [M] [Tout] [Tin] [N1] [N2] [K] [C] [useWeight] [lambda] [I1] [I2] ... [IN1] [J1] [J2] ... [JN2] [test] [query]");
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
	private float[][][][] params;
	
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
	 * @param lambda	regularization parameters
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param printLog
	 * @return [(iteration, elapsed time, trainingRMSE, testRMSE), ...]
	 */
	public double[][] run(Tensor[] training, final int K, int Tout, int Tin, final int M, final int C, final float lambda, final boolean useWeight, boolean printLog){
		
		Random random = new Random();

		final Tensor[] R =  new Tensor[]{training[0].copy(), training[1].copy()}; // residual tensor

		final int[] nnzTraining = new int[]{R[0].omega, R[1].omega}; // |\Omega| number of observable entries in training data
		boolean useTest = test!=null;

		final int N[] = new int[]{R[0].N, R[1].N}; // dimension
		final int[][] modeLengths = new int[][]{R[0].modeLengths, R[1].modeLengths}; // n -> I_{n}

		final int[][][] nnzFiber = new int[][][]{TensorMethods.cardinality(R[0]), TensorMethods.cardinality(R[1])}; // (n, i_{n}) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
		
		/*
		 * distribute the rows of factor matrices across machines
		 */
		final int[][][][] division = new int[][][][]{new int[M][N[0]][], new int[M][N[1]][]}; // (m, n) -> {}_{m}\Omega^{(n)} (list of R entries used to update the factor matrix A^{(n)})
		final int[][][] divisionCount = new int[][][]{new int[M][N[0]], new int[M][N[1]]}; // (m, n) -> |{}_{m}\Omega^{(n)}|
		{
			for(int i=0; i<2; i++) {
				// calculate the number of entries in each division
				for (int elemIdx = 0; elemIdx < R[i].omega; elemIdx++) {
					for (int n = 0; n < N[i]; n++) {
						int bIndex = Math.min(R[i].indices[n][elemIdx] * M / modeLengths[i][n], M - 1);
						divisionCount[i][bIndex][n]++;
					}
				}

				//initialize each division
				for (int m = 0; m < M; m++) {
					for (int n = 0; n < N[i]; n++) {
						division[i][m][n] = new int[divisionCount[i][m][n]];
						divisionCount[i][m][n] = 0;
					}
				}

				//assign entries in each division
				for (int elemIdx = 0; elemIdx < R[i].omega; elemIdx++) {
					for (int n = 0; n < N[i]; n++) {
						int bIndex = Math.min(R[i].indices[n][elemIdx] * M / modeLengths[i][n], M - 1);
						division[i][bIndex][n][divisionCount[i][bIndex][n]++] = elemIdx;
					}
				}
			}
		}
		
		//sort each division so that entries used to update each fiber located contiguously
		for(int i=0; i<2; i++) {
			for (int m = 0; m < M; m++) {
				for (int dim = 0; dim < N[i]; dim++) {
					final int currentDim = dim;
					int[] arrayToSort = division[i][m][dim];
					Integer[] temp = new Integer[arrayToSort.length];
					for (int j = 0; j < arrayToSort.length; j++) {
						temp[j] = arrayToSort[j];
					}
					final int _i = i;
					Arrays.sort(temp, new Comparator<Integer>() {
						public int compare(Integer elemIdx, Integer tElemIdx) {
							int modeIdx = R[_i].indices[currentDim][elemIdx];
							int tModeIdx = R[_i].indices[currentDim][tElemIdx];
							return modeIdx - tModeIdx;
						}
					});
					for (int j = 0; j < arrayToSort.length; j++) {
						arrayToSort[j] = temp[j];
					}
				}
			}
		}

		params = new float[][][][]{new float[N[0]][][],new float[N[1]][][]}; //factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
		for(int i=0; i<2; i++) {
			for (int n = i; n < N[i]; n++) {
				params[i][n] = ArrayMethods.createUniformRandomMatrix(modeLengths[i][n], K, (n == 0) ? 0 : 1, random);
			}
		}
        params[1][0] = params[0][0];

		final float[][][][] currentParams = new float[][][][]{new float[N[0]][][],new float[N[1]][][]};// the columns of factor matrices currently updated (n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}
		for(int i=0; i<2; i++) {
			for (int n = i; n < N[i]; n++) {
				currentParams[i][n] = new float[modeLengths[i][n]][C];
			}
		}
        currentParams[1][0] = currentParams[0][0];

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

						for(int i=0; i<2; i++) {
							for (int dim = 0; dim < N[i]; dim++) {
								int[] indicies = blockIndex(modeLengths[i][dim], M, blockIndex);
								int rowStart = indicies[0];
								int rowEnd = indicies[1];

								for (int row = rowStart; row <= rowEnd; row++) {
									for (int column = cStart; column <= cEnd; column++) {
										currentParams[i][dim][row][column - cStart] = params[i][dim][row][permutedColumns[column]];
									}
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

						for(int i=0; i<2; i++) {
							int[] indicies = blockIndex(nnzTraining[i], M, blockIndex);
							int startIdx = indicies[0];
							int endIdx = indicies[1];

							updateR(R[i], currentParams[i], cLength, startIdx, endIdx, true);
						}
						
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));


				/*
				 * rank C factorization
				 */
				for(int innerIter = 0; innerIter < Tin; innerIter++){

                    new MultiThread<Object>() {

                        @Override
                        public Object runJob(int b, int threadIndex) {

                            int[] indicies = blockIndex(modeLengths[0][0], M, b);
                            int startIdx = indicies[0];
                            int endIdx = indicies[1];
                            updateCoupledFactor(R, new int[][]{division[0][b][0], division[1][b][0]}, currentParams, cLength, lambda, useWeight, startIdx, endIdx, new int[][]{nnzFiber[0][0], nnzFiber[1][0]});

                            return null;
                        }

                    }.run(M, MultiThread.createJobList(M));

					for(int i=0; i<2; i++) {

						final int _i = i;
						for (int dim = 1; dim < N[_i]; dim++) {

							final int currentDim = dim;

							new MultiThread<Object>() {

								@Override
								public Object runJob(int b, int threadIndex) {

									int[] indicies = blockIndex(modeLengths[_i][currentDim], M, b);
									int startIdx = indicies[0];
									int endIdx = indicies[1];
									updateFactor(R[_i], division[_i][b][currentDim], currentDim, currentParams[_i], cLength, lambda, useWeight, startIdx, endIdx, nnzFiber[_i][currentDim]);

									return null;
								}

							}.run(M, MultiThread.createJobList(M));
						}
					}
				}
				
				/*
				 * update R entries
				 */	
				new MultiThread<Object>(){
						
					@Override
					public Object runJob(int b, int threadIndex) {

                        for(int i=0; i<2; i++) {
                            int[] indicies = blockIndex(nnzTraining[i], M, b);
                            int rowStart = indicies[0];
                            int rowEnd = indicies[1];
                            updateR(R[i], currentParams[i], cLength, rowStart, rowEnd, false);
                        }
                        return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
				
				/*
				 * update paramters
				 */	
				new MultiThread<Object>(){
					
					@Override
					public Object runJob(int b, int threadIndex) {

                        for(int i=0; i<2; i++) {
                            for (int dim = i; dim < N[i]; dim++) {
                                int[] indicies = blockIndex(modeLengths[i][dim], M, b);
                                int startIdx = indicies[0];
                                int endIdx = indicies[1];
                                for (int idx = startIdx; idx <= endIdx; idx++) {
                                    for (int column = cStart; column <= cEnd; column++) {
                                        params[i][dim][idx][permutedColumns[column]] = currentParams[i][dim][idx][column - cStart];
                                    }
                                }
                            }
                        }
						
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
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
						int[] indicies = blockIndex(nnzTraining[0], M, b);
						int rowStart = indicies[0];
						int rowEnd = indicies[1];
							
						for(int elemIdx =rowStart; elemIdx <= rowEnd; elemIdx++){
							innerLoss += R[0].values[elemIdx] * R[0].values[elemIdx];
						}
					}
					
					synchronized(loss){
						loss[0]+=innerLoss;
					}
					
					return null;
				} 
			
			}.run(M, MultiThread.createJobList(M));
			
			double trainingRMSE = Math.sqrt(loss[0]/nnzTraining[0]);
			double testRMSE = 0;
			if(useTest){
				testRMSE = Performance.computeRMSE(test, params[0], N[0], K, M);
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
     * @param currentParams	(n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}, the columns of fator matrices currently updated
     * @param cLength	min(C, the number of left rows)
     * @param lambda	regularization parameters
     * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
     * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
     * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
     * @param nnzFiber	(n) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
     */
    private static void updateCoupledFactor(Tensor[] R, int[][] RIndex, float[][][][] currentParams, int cLength, float lambda, boolean useWeight, int firstRow, int lastRow, int[][] nnzFiber){

        int[] dimension = new int[]{currentParams[0].length, currentParams[1].length};
        int numberOfRows = currentParams[0][0].length;

        double[][][] B = new double[numberOfRows][][];
        double[][][] c = new double[numberOfRows][][];
        Queue<Integer> queue = new LinkedList();

        int[] oldResultIndex = new int[]{-1, -1};
        int[] idx = new int[2];

        while(idx[0] < RIndex[0].length || idx[1] < RIndex[1].length) {
            int i = 0;
            if (idx[1] < RIndex[1].length && (idx[0] == RIndex[0].length || oldResultIndex[0] > oldResultIndex[1])) {
                i = 1;
            }

            int elemIdx = RIndex[i][idx[i]++];
            int resultIndex = R[i].indices[0][elemIdx];

            if(oldResultIndex[i] >= 0 && oldResultIndex[i] < resultIndex){

                boolean isProcessed = false;
                Queue<Integer> processed = new LinkedList();

                for (int indexToProcess : queue) {

                    if(indexToProcess < resultIndex) {

                        if (indexToProcess == oldResultIndex[i])
                            isProcessed = true;

                        processed.add(indexToProcess);

                        //symmetrize
                        for (int r1 = 0; r1 < cLength; r1++) {
                            for (int r2 = r1 + 1; r2 < cLength; r2++) {
                                B[indexToProcess][r2][r1] = B[indexToProcess][r1][r2];
                            }
                        }

                        //add identity matrix
                        for (int r = 0; r < cLength; r++) {
                            B[indexToProcess][r][r] += lambda * (useWeight ? nnzFiber[0][indexToProcess] + nnzFiber[1][indexToProcess] : 1);
                        }

                        double[][] newParam = new CholeskyDecomposition(new MatrixJama(B[indexToProcess])).solve(new MatrixJama(c[indexToProcess])).getArray();
                        for (int r = 0; r < cLength; r++) {
                            float result = (float) newParam[r][0];
                            if (result > -epsilon && result < epsilon) {
                                result = 0;
                            }
                            currentParams[0][0][indexToProcess][r] = result;
                        }

                        //clear
                        B[indexToProcess] = null;
                        c[indexToProcess] = null;

                    }
                }

                for(int indexProcessed : processed) {
                    queue.remove(indexProcessed);
                }

                if(!isProcessed)
                    queue.add(oldResultIndex[i]);
            }

            oldResultIndex[i] = resultIndex;

            // \prod_{l \neq n}a^{(l)}_{i_{l}k_{c}}
            double[] product = new double[cLength];
            for(int r=0; r<cLength; r++){
                product[r] = 1;
            }
            int[] indices = new int[dimension[i]];
            for(int dim=0; dim<dimension[i]; dim++){
                indices[dim] = R[i].indices[dim][elemIdx];
            }

            for(int dim=1; dim<dimension[i]; dim++){
                int nextmode = dim;
                for(int r=0; r<cLength; r++){
                    product[r] *= currentParams[i][nextmode][indices[nextmode]][r];
                }
            }

            float value = R[i].values[elemIdx];
            if (B[resultIndex] == null) {
                B[resultIndex] = new double[cLength][cLength];
                c[resultIndex] = new double[cLength][1];
            }
            for(int r1=0; r1<cLength; r1++) {
                for (int r2 = r1; r2 < cLength; r2++) {
                    B[resultIndex][r1][r2] += product[r1] * product[r2];
                }
                c[resultIndex][r1][0] += product[r1] * value;
            }
        }
        if(oldResultIndex[0] >= 0 || oldResultIndex[1] >= 0) {

            if(!queue.contains(oldResultIndex[0]) && B[oldResultIndex[0]] != null) {
                queue.add(oldResultIndex[0]);
            }
            if(!queue.contains(oldResultIndex[1]) && B[oldResultIndex[1]] != null) {
                queue.add(oldResultIndex[1]);
            }

            for (int indexToProcess : queue) {

                //symmetrize
                for (int r1 = 0; r1 < cLength; r1++) {
                    for (int r2 = r1 + 1; r2 < cLength; r2++) {
                        B[indexToProcess][r2][r1] = B[indexToProcess][r1][r2];
                    }
                }

                //add identity matrix
                for (int r = 0; r < cLength; r++) {
                    B[indexToProcess][r][r] += lambda * (useWeight ? nnzFiber[0][indexToProcess] + nnzFiber[1][indexToProcess] : 1);
                }

                double[][] newParam = new CholeskyDecomposition(new MatrixJama(B[indexToProcess])).solve(new MatrixJama(c[indexToProcess])).getArray();
                for (int r = 0; r < cLength; r++) {
                    float result = (float) newParam[r][0];
                    if (result > -epsilon && result < epsilon) {
                        result = 0;
                    }
                    currentParams[0][0][indexToProcess][r] = result;
                }

                //clear
                B[indexToProcess] = null;
                c[indexToProcess] = null;
            }
        }
    }

	
	/**
	 * update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
	 * @param R	residual tensor
	 * @param RIndex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	mode
	 * @param currentParams	(n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}, the columns of fator matrices currently updated
	 * @param cLength	min(C, the number of left rows)
	 * @param lambda	regularization parameters
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
	 * @param lambdas	regularization parameters
	 * @param useWeight	whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	(n) -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	private void updateBiases(Tensor R, int[] Rindex, int n, float[] oldBias, float[] currentBias, float[] lambdas, boolean useWeight, int firstRow, int lastRow, int[] nnzFiber){
		
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
			denominators[i] += lambdas[n] * (useWeight ? nnzFiber[idx] : 1);
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

