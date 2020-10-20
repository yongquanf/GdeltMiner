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
 * Single machine version of Coupled CDTF
 * <P>
 * @author Kijung
 */
public class Coupled_CDTF {

	////////////////////////////////////
	//public methods
	////////////////////////////////////

	public static void main(String[] args) throws Exception{
		run(args);
	}
	
	/**
	 * run Coupled CDTF
	 * @param args	[training] [coupled_tensor] [output] [M] [Tout] [Tin] [N1] [N2] [K] [useWeight] [lambda] [I1] [I2] ... [IN1] [J1] [J2] [JN2] [test] [query]
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
            boolean useWeight = Integer.valueOf(args[9]) > 0;
            System.out.println("-useWeight: "+useWeight);
			float lambda = Float.valueOf(args[10]);
			System.out.println("-lambda: "+lambda);

			int[][] modeSizes = new int[2][];
			modeSizes[0] = new int[N[0]];
			for(int n=0; n<N[0]; n++){
				modeSizes[0][n] = Integer.valueOf(args[11+n]);
				System.out.println("-I"+(n+1)+": "+modeSizes[0][n]);
			}
            modeSizes[1] = new int[N[1]];
			for(int n=0; n<N[1]; n++){
				modeSizes[1][n] = Integer.valueOf(args[11+N[0]+n]);
				System.out.println("-J"+(n+1)+": "+modeSizes[1][n]);
			}
			String test = null;
			if(args.length > 11+N[0]+N[1] ){
				test = args[11+N[0]+N[1]].trim();
				System.out.println("-test: "+test);
			}
			String query = null;
			if(args.length > 12+N[0]+N[1] ){
				query = args[12+N[0]+N[1]].trim();
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

			
			Tensor testTensor = null;
			if(test!=null){
				testTensor = TensorMethods.importSparseTensor(test, ",", modeSizes[0], modesIdx[0], N[0], permutedIdx[0]);
			}
			
			String name = "Coupled-CDTF";
			
			System.out.println("=============");
			System.out.println("Start "+name+"...");
			System.out.println("=============");
			
			Coupled_CDTF method = new Coupled_CDTF();
			
			Tensor[] trainingTensor = new Tensor[]{
			    TensorMethods.importSparseTensor(training, ",", modeSizes[0], modesIdx[0], N[0], permutedIdx[0]),
			    TensorMethods.importSparseTensor(coupled_tensor, ",", modeSizes[1], modesIdx[1], N[1], permutedIdx[1])
            };
			if(test!=null){
				method.setTest(testTensor);
			}
			
			result = method.run(trainingTensor, K, Tout, Tin, M, lambda, useWeight, true);
			
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
				String fileName = "run_single_coupled_cdtf.sh";
				System.err.println("Usage: " + fileName + " [training] [coupled_tensor] [output] [M] [Tout] [Tin] [N1] [N2] [K] [useWeight] [lambda] [I1] [I2] ... [IN1] [J1] [J2] [JN2] [test] [query]");
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
	 * run CDTF
	 * 
	 * @param training	training data
	 * @param K	rank
	 * @param Tout	number of outer iterations
	 * @param Tin	number of inner iterations
	 * @param M	number of cores
	 * @param lambda	regularization parameter
     * @param useWeight
	 * @param printLog	whether to print logs
	 * @return statistics per iteration [(iteration, elapsed time, trainingRMSE, testRMSE), ...]
	 */
	public double[][] run(Tensor[] training, final int K, int Tout, int Tin, final int M, final float lambda, final boolean useWeight, boolean printLog){
		
		Random random = new Random();
		
		if(printLog){
			System.out.println("iteration,elapsed_time,training_rmse,test_rmse");
		}
		
		final Tensor[] R =  new Tensor[]{training[0].copy(), training[1].copy()}; // residual tensor
		
		final int[] nnzTraining = new int[]{R[0].omega, R[1].omega}; // |\Omega| number of observable entries in training data
		boolean useTest = test!=null;
		
		final int N[] = new int[]{R[0].N, R[1].N}; // dimension
		final int[][] modeLength = new int[][]{R[0].modeLengths, R[1].modeLengths}; // n -> I_{n}
		
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
                        int bIndex = Math.min(R[i].indices[n][elemIdx] * M / modeLength[i][n], M - 1);
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
                        int bIndex = Math.min(R[i].indices[n][elemIdx] * M / modeLength[i][n], M - 1);
                        division[i][bIndex][n][divisionCount[i][bIndex][n]++] = elemIdx;
                    }
                }
            }
		}
		
		params = new float[][][][]{new float[N[0]][][],new float[N[1]][][]}; //factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
        for(int i=0; i<2; i++) {
            for (int n = i; n < N[i]; n++) {
                params[i][n] = ArrayMethods.createUniformRandomMatrix(modeLength[i][n], K, (n == 0) ? 0 : 1, random);
            }
        }
        params[1][0] = params[0][0];

		final float[][][] updatedColumn = new float[][][]{new float[N[0]][], new float[N[1]][]}; // the columns of factor matrices currently updated
        for(int i=0; i<2; i++) {
            for (int n = i; n < N[i]; n++) {
                updatedColumn[i][n] = new float[modeLength[i][n]];
            }
        }
        updatedColumn[1][0] = updatedColumn[0][0];


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

                        for(int i=0; i<2; i++) {
                            for (int n = i; n < N[i]; n++) {
                                int[] indicies = blockIndex(modeLength[i][n], M, blockIndex);
                                int rowStart = indicies[0];
                                int rowEnd = indicies[1];

                                for (int row = rowStart; row <= rowEnd; row++) {
                                    updatedColumn[i][n][row] = params[i][n][row][_k];
                                }

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

                        for(int i=0; i<2; i++) {
                            int[] indicies = blockIndex(nnzTraining[i], M, blockIndex);
                            int startIdx = indicies[0];
                            int endIdx = indicies[1];
                            updateR(R[i], updatedColumn[i], startIdx, endIdx, true);
                        }
						return null;
					} 
				
				}.run(M, MultiThread.createJobList(M));
				
				
				for(int innerIter = 0; innerIter < Tin; innerIter++){

					/*
					 * rank one factorization
					 */
                    new MultiThread<Object>() {

                        @Override
                        public Object runJob(int m, int threadIndex) {

                            int[][] indicies = new int[][]{blockIndex(modeLength[0][0], M, m), blockIndex(modeLength[1][0], M, m)};
                            int startIdx = indicies[0][0];
                            int endIdx = indicies[0][1];

                            // update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
                            updateCoupledFactors(R, new int[][]{division[0][m][0], division[1][m][0]}, updatedColumn, lambda, useWeight, startIdx, endIdx, new int[][]{nnzFiber[0][0], nnzFiber[1][0]});

                            return null;
                        }

                    }.run(M, MultiThread.createJobList(M));

                    for(int i=0; i<2; i++) {
                        final int _i = i;

                        for (int n = 1; n < N[i]; n++) {
                            final int _n = n;

                            new MultiThread<Object>() {

                                @Override
                                public Object runJob(int m, int threadIndex) {

                                    int[] indicies = blockIndex(modeLength[_i][_n], M, m);
                                    int startIdx = indicies[0];
                                    int endIdx = indicies[1];

                                    // update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
                                    updateFactors(R[_i], division[_i][m][_n], _n, updatedColumn[_i], lambda, useWeight, startIdx, endIdx, nnzFiber[_i][_n]);

                                    return null;
                                }

                            }.run(M, MultiThread.createJobList(M));
                        }
                    }
				}
				
				/*
				 * update R entries and parameters
				 */
				new MultiThread<Object>(){
					
					@Override
					public Object runJob(int b, int threadIndex) {

                        for(int i=0; i<2; i++) {
                            {
                                int[] indicies = blockIndex(nnzTraining[i], M, b);
                                int rowStart = indicies[0];
                                int rowEnd = indicies[1];
                                updateR(R[i], updatedColumn[i], rowStart, rowEnd, false);
                            }

                            for (int n = i; n < N[i]; n++) {
                                int[] indicies = blockIndex(modeLength[i][n], M, b);
                                int startIdx = indicies[0];
                                int endIdx = indicies[1];
                                for (int idx = startIdx; idx <= endIdx; idx++) {
                                    params[i][n][idx][_k] = updatedColumn[i][n][idx];
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
	 * @param Rindex	indices of R entries in {}_{m}\Omega^{(n)}
	 * @param n	updated mode
	 * @param updatedColumn	n -> a^{(n)}{i_{n}*}, the columns of fator matrices currently updated
	 * @param lambda	regularization parameter
     * @param useWeight
	 * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
	 * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
	 * @param nnzFiber	i_{n} -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
	 */
	public void updateFactors(Tensor R, int[] Rindex, int n, float[][] updatedColumn, float lambda, boolean useWeight, int firstRow, int lastRow, int[] nnzFiber){
		
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

        for (int i = 0; i < numberOfRows; i++) {
            int idx = i + firstRow;
            denominators[i] += lambda * (useWeight ? nnzFiber[idx] : 1);
            if (denominators[i] != 0) {
                float result = numerators[i] / denominators[i];
                if (result > -epsilon && result < epsilon) { // to prevent underflow
                    result = 0;
                }
//                System.out.println(numerators[i] +"," + denominators[i] + "," + updatedColumn[n][idx]+","+result);
                updatedColumn[n][idx] = result;
            }
        }
	}

    /**
     * update {}_{m}a^{(n)}{i_{n}*}, the parameters in the kth column of the factor matrix A^{(n)} assigned to machine m
     * @param R	residual tensor
     * @param Rindex	indices of R entries in {}_{m}\Omega^{(n)}
     * @param updatedColumn	n -> a^{(n)}{i_{n}*}, the columns of fator matrices currently updated
     * @param lambda	regularization parameter
     * @param useWeight
     * @param firstRow	first row of the factor matrix A^{(n)} assigned to this machine
     * @param lastRow	last row of the factor matrix A^{(n)} assigned to this machine
     * @param nnzFiber	i_{n} -> |\Omega^{(n)}_{i_{n}|, number of observable entries in each fiber
     */
    public void updateCoupledFactors(Tensor R[], int[][] Rindex, float[][][] updatedColumn, float lambda, boolean useWeight, int firstRow, int lastRow, int[][] nnzFiber){

        int[] dimension = new int[]{updatedColumn[0].length, updatedColumn[1].length};
        int numberOfRows = lastRow - firstRow+1;
        float[] numerators = new float[numberOfRows];
        float[] denominators = new float[numberOfRows];

        for(int i=0; i<2; i++) {
            for (int idx = 0; idx < Rindex[i].length; idx++) {

                int elemIdx = Rindex[i][idx];
                float numerator = 1;
                float denominator = 1;
                for (int n = 0; n < dimension[i]; n++) {
                    if (n != 0)
                        numerator *= updatedColumn[i][n][R[i].indices[n][elemIdx]];
                }
                denominator = numerator * numerator;
                numerator *= R[i].values[elemIdx];
                int resultIndex = R[i].indices[0][elemIdx] - firstRow;
                numerators[resultIndex] += numerator;
                denominators[resultIndex] += denominator;
            }
        }

        for (int i = 0; i < numberOfRows; i++) {
            int idx = i + firstRow;
            denominators[i] += lambda * (useWeight ? (nnzFiber[0][idx] + nnzFiber[1][idx]) : 1);
            if (denominators[i] != 0) {
                float result = numerators[i] / denominators[i];
                if (result > -epsilon && result < epsilon) { // to prevent underflow
                    result = 0;
                }
                updatedColumn[0][0][idx] = result;
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
            for (int idx = startIdx; idx <= endIdx; idx++) {
                float product = 1;
                for (int n = 0; n < updatedColumn.length; n++) {
                    product = product * updatedColumn[n][R.indices[n][idx]];
                }
                R.values[idx] = R.values[idx] + product;
            }
		}
		else{
            for (int idx = startIdx; idx <= endIdx; idx++) {
                float product = 1;
                for (int n = 0; n < updatedColumn.length; n++) {
                    product = product * updatedColumn[n][R.indices[n][idx]];
                }
                R.values[idx] = R.values[idx] - product;
            }
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

