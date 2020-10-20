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

import static util.tensor.sals.mr.Params.P_AVERAGE;
import static util.tensor.sals.mr.Params.P_C;
import static util.tensor.sals.mr.Params.P_SEED;
import static util.tensor.sals.mr.Params.P_USE_BIAS;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import util.tensor.Jama.CholeskyDecomposition;
import util.tensor.Jama.MatrixJama;

/**
 * Reducer for the Hadoop version of SALS
 * <P>
 * @author Kijung
 */
public class SALSReducer extends CommonReducer {

	////////////////////////////////////
	//private fields
	////////////////////////////////////

	private boolean useBias = false; // whether to use bias terms
	private float[][][] curCols; // the columns of factor matrices currently updated (n, i_{n}, c) -> a^{(n)}_{i_{n}k_{c}}
	private int C; //number of columns updated at a time
	private float mu = 0; //the average of the training entries

	////////////////////////////////////
	//public methods
	////////////////////////////////////

	/**
	 * load parameters and initialize resources
	 * @param context
	 */
	@Override
	public void setup(Context context){

		super.setup(context);

		C = conf.getInt(P_C, 0);
		curCols = new float[N][][];
		for(int mode=0; mode<N; mode++){
			curCols[mode] = new float[modeLengths[mode]+1][C];
		}

		useBias = conf.getBoolean(P_USE_BIAS, false);
		if(useBias){
			mu = conf.getFloat(P_AVERAGE, 0);
		}

	}

	/**
	 * Cache distributed data to local disk
	 * @param key <machine to assign this entry, order of the factor matrix that this entry is used to update>
	 * @param values list of <indices of the entry, value of the entry>
	 * @param context
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	@Override
	public void reduce(TripleWritable key, Iterable<ElementWritable> values, Context context){

		if(machineId < 0){ //first

			machineId = key.left;

			/*
			 * Initialize Local Path
			 */
			String userHome = System.getProperty("user.home");
			baseLocalPath = userHome+"/SALS"+machineId;
			tempLocalFile = baseLocalPath+"/TEMP";
			File baseDir = new File(baseLocalPath);

			if (baseDir.exists())
				try {
					FileUtil.fullyDelete(baseDir);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			baseDir.mkdir(); 

			for(int dim=0; dim<N; dim++){
				File f = new File(getLocalParamPath(dim));
				if(f.exists())
					f.delete();
				f.mkdir();
			}


			try{
				for(int mode=0; mode<N; mode++){
					outIndexR[mode] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(mode, true, CommonMapper.TYPE_TRAINING, false))));
					outValueR[mode] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(mode, false, CommonMapper.TYPE_TRAINING, false))));
				}
				outIndexRTest[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, true, CommonMapper.TYPE_TEST, false))));
				outValueRTest[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, false, CommonMapper.TYPE_TEST, false))));
				outIndexRQuery[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, true, CommonMapper.TYPE_QUERY, false))));
				outValueRQuery[0] = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalRPath(0, false, CommonMapper.TYPE_QUERY, false))));
			} catch(Exception e){
				e.printStackTrace();
			}

			for(int mode=0; mode<N; mode++){
				startIndex[mode] = getStartIndex(mode, machineId);
				endIndex[mode] = getStartIndex(mode, machineId+1);
				if(machineId==M){
					endIndex[mode] +=1;
				}
				nnzFiber[mode] = new int[endIndex[mode] - startIndex[mode]];
			}
		}

		try {
			int fileMode = key.mid;
			for(ElementWritable value : values){
				if(value.isTraining){ //training
					nnzTraining[fileMode]++;
					int[] index = value.index;
					for(int dim = 0; dim <N; dim++){
						outIndexR[fileMode].writeInt(index[dim]);	
					}
					outValueR[fileMode].writeFloat(value.value-mu);
					nnzFiber[fileMode][index[fileMode]-startIndex[fileMode]]++;
				}
				else {

					if(Float.isNaN(value.value)){ //query
						nnzQuery[fileMode]++;
						int[] index = value.index;
						for(int dim = 0; dim <N; dim++){
							outIndexRQuery[fileMode].writeInt(index[dim]);	
						}
						outValueRQuery[fileMode].writeFloat(-mu);
					}
					else {
						nnzTest[fileMode]++; //test
						int[] index = value.index;
						for(int dim = 0; dim <N; dim++){
							outIndexRTest[fileMode].writeInt(index[dim]);	
						}
						outValueRTest[fileMode].writeFloat(value.value-mu);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * SALS main procedure
	 * @param context
	 */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{

		try{
			for(int i=0; i<outIndexR.length; i++){
				outIndexR[i].close();
				outValueR[i].close();
			}
			outIndexRTest[0].close();
			outValueRTest[0].close();
			outIndexRQuery[0].close();
			outValueRQuery[0].close();
		} catch(Exception e){
			e.printStackTrace();
		}

		/*
		 * initialize parameters
		 */
		Random rand = new Random(conf.getInt(P_SEED, 0));
		for(int dim=0; dim<N; dim++){
			context.progress();
			createParamMatrix(dim, K, dim==0 ? 0 : 1, rand, context);
		}

		if(useBias){
			for(int n=0; n<N; n++){
				context.progress();
				createBiasTerms(n, context);
			}
		}

		double[][] result = new double[Tout][6]; //performance statistics

		long startTime = System.currentTimeMillis();

		for(int outIter = 0; outIter<Tout; outIter++){	

			FileSystem fs = FileSystem.get(conf);

			final int[] permutedColumns = createRandomSequence(K, rand);

			for(int splitIter =0; splitIter < Math.ceil((K+0.0)/C); splitIter++){

				final int cStart = C * splitIter;
				final int cEnd = Math.min(C * (splitIter +1) - 1, K - 1);
				final int cLength = cEnd - cStart + 1;

				System.out.printf("Iter : %d, column sequence : %d\n", outIter, splitIter);

				//load C columns of the factor matrices
				for(int n=0; n<N; n++){
					for(int column = cStart; column <= cEnd; column++){
						long time = System.currentTimeMillis();
						context.progress();
						loadFromLocal(n, permutedColumns[column], column-cStart);
						context.getCounter("Speed", "Initialize").increment(System.currentTimeMillis()-time);	
					}
				}

				/*
				 * calculate \hat_{R}
				 */
				for(int n=0; n<N; n++){
					context.progress();

					long time = System.currentTimeMillis();

					if(n==0){
						updateR(n, CommonMapper.TYPE_TRAINING, true, false);
						updateR(n, CommonMapper.TYPE_TEST, true, false);
						updateR(n, CommonMapper.TYPE_QUERY, true, false);
					}
					else {
						updateR(n, CommonMapper.TYPE_TRAINING, true, false);
					}
					context.getCounter("Speed", "Update R_hat").increment(System.currentTimeMillis()-time);
				}

				//inner iteration
				for(int innerIter = 0; innerIter<Tin; innerIter++){

					/*
					 *  rank C factorization
					 */
					for(int n=0; n<N; n++){

						context.progress();
						long time = System.currentTimeMillis();

						// update a^{(n)}_{*k_{1}}, ..., a^{(n)}_{*k_{C}}: the k_{1}th, ..., K_{C}th column of the A^{(n)}
						updateFactors(n, cLength, context);
						context.getCounter("Speed", "Optimize").increment(System.currentTimeMillis()-time);

						// broadcast updated parameters
						time = System.currentTimeMillis();
						communicate(outIter, splitIter, cLength, innerIter, n, context, fs);
						context.getCounter("Speed", "Broadcast").increment(System.currentTimeMillis()-time);
					}
				}

				float errorTrainingSum = 0;
				float errorTestSum = 0;
				int nnzTrainingSum = 0;
				int nnzTestSum = 0;

				/*
				 * Update R
				 */
				for(int n=0; n<N; n++){
					context.progress();
					long time = System.currentTimeMillis();
					if(n==0){
						errorTrainingSum += updateR(n, CommonMapper.TYPE_TRAINING, false, !useBias&&cEnd==K-1);
						errorTestSum += updateR(n, CommonMapper.TYPE_TEST, false, !useBias&&cEnd==K-1);
						updateR(n, CommonMapper.TYPE_QUERY, false, false);
						nnzTrainingSum += nnzTraining[n];
						nnzTestSum += nnzTest[n];
					}
					else {
						updateR(n, CommonMapper.TYPE_TRAINING, false, false);
					}	
					context.getCounter("Speed", "Update R").increment(System.currentTimeMillis()-time);
					time = System.currentTimeMillis();
					for(int column = cStart; column <= cEnd; column++){
						context.progress();
						writeFactors(n, permutedColumns[column], column-cStart);
					}
					context.getCounter("Speed", "Update Param").increment(System.currentTimeMillis()-time);
				}


				if(!useBias&&cEnd==K-1){
					System.out.println(Math.sqrt(errorTrainingSum/nnzTrainingSum));
					System.out.println(Math.sqrt(errorTestSum/nnzTestSum));
					result[outIter] = new double[]{outIter, System.currentTimeMillis()-startTime, errorTrainingSum, nnzTrainingSum, errorTestSum, nnzTestSum};
				}
			}

			// update bias terms
			if(useBias){

				for(int n=0; n<N; n++){

					long time = System.currentTimeMillis();
					context.progress();
					loadBiasFromLocal(n);
					context.getCounter("Speed", "Initialize").increment(System.currentTimeMillis()-time);

					time = System.currentTimeMillis();
					oldBias = curBias.clone();

					context.progress();

					// update b^{(n)}
					updateBias(n);
					context.getCounter("Speed", "Optimize").increment(System.currentTimeMillis()-time);

					time = System.currentTimeMillis();

					// broadcast updated parameters
					communicateBias(outIter, n, context, fs);
					context.getCounter("Speed", "Broadcast").increment(System.currentTimeMillis()-time);


					float trainErrorSum = 0;
					float testErrorSum = 0;
					int NNZSum = 0;
					int NNZTestSum = 0;

					/*
					 * update R
					 */
					for(int nr=0; nr<N; nr++){
						context.progress();

						time = System.currentTimeMillis();

						if(nr==0){
							trainErrorSum += updateRWithBias(nr, n, CommonMapper.TYPE_TRAINING, n==N-1);
							testErrorSum += updateRWithBias(nr, n, CommonMapper.TYPE_TEST, n==N-1);
							updateRWithBias(nr, n, CommonMapper.TYPE_QUERY, false);
							NNZSum += nnzTraining[nr];
							NNZTestSum += nnzTest[nr];
						}
						else {
							updateRWithBias(nr, n, CommonMapper.TYPE_TRAINING, false);
						}
						context.getCounter("Speed", "Update R").increment(System.currentTimeMillis()-time);
						time = System.currentTimeMillis();
					}

					writeBiasParams(n);
					context.getCounter("Speed", "Update Param").increment(System.currentTimeMillis()-time);

					if(n==N-1){
						System.out.println(Math.sqrt(trainErrorSum/NNZSum));
						System.out.println(Math.sqrt(testErrorSum/NNZTestSum));
						result[outIter] = new double[]{outIter, System.currentTimeMillis()-startTime, trainErrorSum, NNZSum, testErrorSum, NNZTestSum};

					}
				}
			}

			if(machineId==0){
				context.getCounter("Time ", ""+outIter).increment(System.currentTimeMillis()-startTime);
			}

			try{
				fs.close();
			} catch(Exception e){};


		}

		nnzFiber = null;
		curCols = null;

		//write estimate
		if(nnzQuery[0]>0){
			writeEstimate(context);
		}

		//write performance statistics
		writePerformance(context, result);

		//write factor matrices
		if(machineId==0){
			writeFactormatricesResult(context);
			if(useBias)
				writeBiasesResults(context);
		}

		//delete temporary files
		File file = new File(baseLocalPath);
		if(file.exists()){
			try {
				FileUtil.fullyDelete(file);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Update factor matrices
	 * Update a^{(n)}_{*k_{1}}, ..., a^{(n)}_{*k_{C}}: the k_{1}th, ..., K_{C}th column of the A^{(n)}
	 * @param n	mode
	 * @param C	number of columns updated at a time
	 * @param context
	 */
	private void updateFactors(int n, int C, Context context) throws IOException {

		double[][] B = new double[C][C]; 
		double[][] c = new double[C][1]; 

		int oldResultIndex = -1;

		ObjectInputStream inIndex = null;
		ObjectInputStream inValue = null;

		inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, CommonMapper.TYPE_TRAINING, false))));
		inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, CommonMapper.TYPE_TRAINING, false))));
		for(int elem=0; elem<nnzTraining[n]; elem++){

			int[] index = new int[N];
			float r = 0;
			for(int _mode=0; _mode<N; _mode++){
				index[_mode] = inIndex.readInt();
			}
			r = inValue.readFloat();

			int resultIndex = index[n];

			if(oldResultIndex>=0 && oldResultIndex!=resultIndex){

				if(oldResultIndex%1000000==0)
					context.progress();

				//symmetrize
				for(int column1=0; column1<C; column1++){
					for(int column2=column1+1; column2<C; column2++){
						B[column2][column1] = B[column1][column2];
					}
				}

				//add identity matrix
				for(int column=0; column<C; column++){
					B[column][column] += lambda * (useWeight ? nnzFiber[n][oldResultIndex-startIndex[n]] : 1);
				}

				try {
					double[][] newParam = new CholeskyDecomposition(new MatrixJama(B)).solve(new MatrixJama(c)).getArray();
					for(int column=0; column<C; column++){
						float result = (float)newParam[column][0];
						if(result > -epsilon && result < epsilon){
							result = 0;
						}
						curCols[n][oldResultIndex][column] = result;
					}
				}catch (Exception e){
					System.out.println("Singular matrix");
				}

				//clear
				B = new double[C][C];
				c = new double[C][1]; 
			}

			oldResultIndex = resultIndex;

			double[] product = new double[C];
			for(int column=0; column<C; column++){
				product[column] = 1;
			}
			for(int i=1; i<N; i++){
				int nextmode = (n+i)%N;
				for(int column=0; column<C; column++){
					product[column] *= curCols[nextmode][index[nextmode]][column];
				}
			}

			for(int column1=0; column1<C; column1++){
				for(int column2=column1; column2<C; column2++){
					B[column1][column2] += product[column1] * product[column2];
				}
				c[column1][0] += product[column1] * r;
			}
		}

		if(oldResultIndex>=0){

			//symmetrize
			for(int column1=0; column1<C; column1++){
				for(int column2=column1+1; column2<C; column2++){
					B[column2][column1] = B[column1][column2];
				}
			}

			for(int column=0; column<C; column++){
				B[column][column] += lambda * (useWeight ? nnzFiber[n][oldResultIndex-startIndex[n]] : 1);
			}

			double[][] newParam = new CholeskyDecomposition(new MatrixJama(B)).solve(new MatrixJama(c)).getArray();
			for(int column=0; column<C; column++){
				float result = (float)newParam[column][0];
				if(result > -epsilon && result < epsilon){
					result = 0;
				}
				curCols[n][oldResultIndex][column] = result;
			}

			//clear
			B = new double[C][C];
			c = new double[C][1]; 
		}

		inIndex.close();
		inValue.close();
	}

	/**
	 * update R entries {}_{m}R^{(n)}
	 * @param n	mode
	 * @param type training / test / query
	 * @param add	true: update R / false: create \hat{R} 
	 * @param measureCost whether to calculate error
	 * @return sum of squared error
	 * @throws IOException
	 */
	public double updateR(int n, int type, boolean add, final boolean measureCost) throws IOException{

		double errorSum = 0;

		ObjectInputStream inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, type, false))));
		ObjectInputStream inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, type, false))));
		ObjectOutputStream outValue = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(getLocalRPath(n, false, type, true))));

		int nnz = 0;
		if(type==CommonMapper.TYPE_TRAINING)
			nnz = nnzTraining[n];
		else if(type==CommonMapper.TYPE_TEST)
			nnz = nnzTest[n];
		else
			nnz = nnzQuery[n];

		if(add){
			for(int elem=0; elem<nnz; elem++){
				int[] index = new int[N];
				for(int _mode=0; _mode<N; _mode++){
					index[_mode] = inIndex.readInt();
				}
				float r = inValue.readFloat();

				float[] product = new float[C];
				for(int column=0; column<C; column++){
					product[column] = 1;
				}
				for(int dim=0; dim<N; dim++){
					for(int column=0; column<C; column++){
						product[column] *= curCols[dim][index[dim]][column];
					}
				}

				for(int column=0; column<C; column++){
					r += product[column];
				}
				outValue.writeFloat(r);
			}
		}
		else {
			for(int elem=0; elem<nnz; elem++){
				int[] index = new int[N];
				for(int _mode=0; _mode<N; _mode++){
					index[_mode] = inIndex.readInt();
				}
				float r = inValue.readFloat();

				float[] product = new float[C];
				for(int column=0; column<C; column++){
					product[column] = 1;
				}
				for(int dim=0; dim<N; dim++){
					for(int column=0; column<C; column++){
						product[column] *= curCols[dim][index[dim]][column];
					}
				}

				for(int column=0; column<C; column++){
					r -= product[column];
				}
				outValue.writeFloat(r);

				if(measureCost)
					errorSum += r*r;
			}
		}


		inIndex.close(); 
		inValue.close();
		outValue.close();

		replace(getLocalRPath(n, false, type, false));

		return errorSum;
	}

	/**
	 * send updated parameters to other machines and receive updated parameters from other machines.
	 * braodcast {}_{m}a^{(n)}_{*k_{1}},..., {}_{m}a^{(n)}_{*k_{C}} and receive a^{(n)}_{*k_{1}}, ..., a^{(n)}_{*k_{C}} 
	 * @param outIter
	 * @param splitIter
	 * @param inIter
	 * @param C	number of columns updated at a time
	 * @param n	order of factor matrix
	 * @param context
	 * @param fs
	 */
	private void communicate(int outIter, int splitIter, int C, int inIter, int n, Context context, FileSystem fs) throws IOException{

		FSDataOutputStream out = null;

		//write mine
		Path outPath = new Path(getHDFSParamPath(outIter, splitIter, inIter, n, machineId, false));
		out = fs.create(outPath);
		for(int i=startIndex[n]; i<endIndex[n]; i++){
			for(int column=0; column<C; column++){
				out.writeFloat(curCols[n][i][column]);
			}
		}
		out.close();

		//mark write
		markWrite(outIter, splitIter, inIter, n, fs);

		//check if others write their file and read it
		boolean[] markReadComplete = new boolean[M];
		markReadComplete[machineId] = true;
		while(true){

			long requestTime = System.currentTimeMillis();
			FileStatus[] statusList = fs.listStatus(new Path(getHDFSParamPath(outIter, splitIter, inIter, n, true)));
			shuffle(statusList);

			for(FileStatus status : statusList){

				int _taskId = Integer.valueOf(status.getPath().getName());

				if(markReadComplete[_taskId])
					continue;
				else {
					FSDataInputStream in = null;
					try{
						in = fs.open(new Path(getHDFSParamPath(outIter, splitIter, inIter, n, _taskId, false)));
						for(int i=getStartIndex(n, _taskId); i<getStartIndex(n, _taskId+1); i++){
							for(int column=0; column<C; column++){
								curCols[n][i][column]=in.readFloat();
							}
						}
					} catch(Exception e){
						System.out.println(e.getMessage());
						context.getCounter("Error", "err").increment(1);
						continue;
					} finally{
						try { in.close(); } catch (Exception e) {}
					}
					markReadComplete[_taskId]=true;
				}
			}

			boolean markAll = true;
			for(int _taskId=0; _taskId<M; _taskId++){
				if(!markReadComplete[_taskId]){
					markAll = false;
					break;
				}
			}

			if(markAll){
				break;
			}
			else {
				context.progress();
				long timeToWait = (long)(waiting * Math.random()) - (System.currentTimeMillis() - requestTime); //avoid simultaneous request
				if(timeToWait > 0){
					try {
						Thread.sleep(timeToWait);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				continue;
			}
		}
	}

	/**
	 * load parameters a^{(n)}_{*k_{c}}
	 * @param n	mode
	 * @param k	column
	 * @param c position in updated columns
	 */
	private void loadFromLocal(int n, int k, int c) throws IOException{

		ObjectInputStream in =  new ObjectInputStream(new BufferedInputStream (new FileInputStream(getLocalParamPath(n, k, false))));
		for(int i=0; i<modeLengths[n]; i++){
			curCols[n][i][c]=in.readFloat();
		}
		in.close();
	}

	/**
	 * create a randomly shuffled sequence (0 through n-1)
	 * @param n	length of sequence
	 * @param rand random number generator
	 * @return	created sequence
	 */
	private static int[] createRandomSequence(int n, Random rand){
		int[] result = new int[n];
		for(int i=0; i<n; i++){
			result[i] = i;
		}
		shuffle(result, rand);
		return result;
	}

	/**
	 * shuffle given vector
	 * @param vec
	 * @param rand random number generator
	 */
	private static void shuffle(int[] vec, Random rand){
		int n = vec.length;
		for (int i = 0; i < n; i++){
			int randI = rand.nextInt(n-i);
			int temp = vec[n-i-1];
			vec[n-i-1] = vec[randI];
			vec[randI] = temp;
		}
	}

	/**
	 * Write factors
	 * Write a^{(n)}_{*k_{1}}, ..., a^{(n)}_{*k_{C}}: the k_{1}th, ..., K_{C}th column of the A^{(n)}
	 * @param n mode
	 * @param k column
	 * @param c position in updated columns
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private void writeFactors(int n, int k, int c) throws FileNotFoundException, IOException{
		ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalParamPath(n, k, false))));
		for(int row=0; row<modeLengths[n]; row++){
			os.writeFloat(curCols[n][row][c]);
		}
		os.close();
	}

}
