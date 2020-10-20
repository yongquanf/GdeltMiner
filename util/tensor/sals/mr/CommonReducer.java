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

import static util.tensor.sals.mr.Params.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Common interface of reducer classes
 * <P>
 * @author Kijung
 */
public abstract class CommonReducer extends Reducer<TripleWritable, ElementWritable, NullWritable, NullWritable> {

	////////////////////////////////////
	// private & protected fields
	////////////////////////////////////
	
	protected float epsilon = 0.000000000001f;
	
	protected int machineId; // reducer ID
	protected String baseLocalPath; // directory in local disk to cache distributed data
	protected String baseHDFSPath; // directory in HDFS to save temporary data for communication
	
	protected Configuration conf;
	
	protected float[] oldBias; // values of biases before update
	protected float[] curBias; // values of biases after update
	
	protected int K; //rank
	protected int M; // number of reducer
	protected int Tout; // number of outer iterations 
	protected int Tin; // number of inner iterations
	protected float lambda; //regularization parameter
	protected float lambdaForBiases; //regularization parameter
	protected int regularization; // regularization method (1: L1-regularization, 2: L2-regularization)
	protected boolean useWeight; //whether to use weighted lambda regularization (true: weighted lambda regularization, false: L2 regularization)
	protected boolean nonNegative; // whether to use nonnegativity constraint
	protected int waiting; // interval between hdfs update checks
	
	protected int N; // dimension
	protected int[] modeLengths; // n -> I_{n}
	protected int[][] nnzFiber; // (n, i_{n}) -> |\Omega^{(n)}_{i_{n}}|
	protected int[] nnzTraining; // n -> |{}_{m}R^{n}|, number of observable elements in training data
	protected int[] nnzTest; // number of observable elements in test data
	protected int[] nnzQuery; // number of observable elements in query data
	
	protected int[] startIndex; // the first row of each factor matrix assigned to this machine
	protected int[] endIndex; // the last row of each factor matrix assigned to this machine
	
	protected ObjectOutputStream[] outIndexR; //file output stream to write indices of R entries
	protected ObjectOutputStream[] outValueR; //file output stream to write value of R entries
	protected ObjectOutputStream[] outIndexRTest; //file output stream to write indices of R entries
	protected ObjectOutputStream[] outValueRTest; //file output stream to write value of R entries
	protected ObjectOutputStream[] outIndexRQuery; //file output stream to write indices of R entries
	protected ObjectOutputStream[] outValueRQuery; //file output stream to write value of R entries
	
	protected String tempLocalFile; // path to tempoaray local file
	
	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * load parameters and initialize resources
	 * @param context
	 */
	@Override
	public void setup(Context context){
		
		conf = context.getConfiguration();
		
		N = conf.getInt(P_N, 0);
		ElementWritable.N = N;
		
		modeLengths = new int[N];
		nnzFiber = new int[N][];
		nnzTraining = new int[N];
		nnzTest = new int[1];
		nnzQuery = new int[1];
		
		startIndex = new int[N];
		endIndex = new int[N];
		
		outIndexR = new ObjectOutputStream[N];
		outValueR = new ObjectOutputStream[N];
		outIndexRTest = new ObjectOutputStream[1];
		outValueRTest = new ObjectOutputStream[1];
		outIndexRQuery = new ObjectOutputStream[1];
		outValueRQuery = new ObjectOutputStream[1];
		
		for(int i=0; i<N; i++){
			modeLengths[i] = conf.getInt(P_MODELENGTH+(i+1), 0);
		}
		
		M = conf.getInt(P_M, 0);
		K = conf.getInt(P_K, 0);
		Tout = conf.getInt(P_TOUT, 0);
		Tin = conf.getInt(P_TIN, 0);
		baseHDFSPath = conf.get(P_OUTPUT);
		waiting = conf.getInt(P_WAITING, 100);
		lambda = conf.getFloat(P_LAMBDA, 0);
        lambdaForBiases = conf.getFloat(P_LAMBDA_BIAS, 0);
        regularization = conf.getInt(P_REGULARIZATION, 0);
        useWeight = conf.getBoolean(P_USE_WEIGHT, true);
        nonNegative = conf.getBoolean(P_NONNEGATIVE, false);
		machineId = -1;
	
	}
	
	////////////////////////////////////
	// private & protected methods
	////////////////////////////////////
	
	/**
	 * Update bias terms (b^{(n)})
	 * @param n	mode
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void updateBias(int n) throws FileNotFoundException, IOException {

		int blockLength = endIndex[n] - startIndex[n];
		float[] numerators = new float[blockLength];
		float[] denominators = new float[blockLength];

		ObjectInputStream inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, CommonMapper.TYPE_TRAINING, false))));
		ObjectInputStream inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, CommonMapper.TYPE_TRAINING, false))));

		for(int elem=0; elem<nnzTraining[n]; elem++){
			int[] index = new int[N];
			for(int _mode=0; _mode<N; _mode++){
				index[_mode] = inIndex.readInt();
			}
			float r = inValue.readFloat();
			int resultIndex = index[n]-startIndex[n];
			numerators[resultIndex] += r + oldBias[index[n]];
			denominators[resultIndex] += 1;
		}

		inIndex.close(); 
		inValue.close();


		for(int i=0; i<blockLength; i++){
			if(denominators[i]!=0){
				denominators[i] += lambdaForBiases * (useWeight ? nnzFiber[n][i] : 1);
				int rowIndex = i+startIndex[n];
				float result = numerators[i] / denominators[i];
				if(result > -epsilon && result < epsilon){ // to prevent underflow
					result = 0;
				}
				curBias[rowIndex] = result;
			}
		}
	}

	/**
	 * update R entries {}_{m}R^{(n)} after updating bias terms
	 * @param n	mode
	 * @param nBias	mode of updated bias terms  
	 * @param type training / test / query
	 * @param measureCost whether to calculate error
	 * @return sum of errors
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected double updateRWithBias(int n, int nBias, int type, final boolean measureCost) throws FileNotFoundException, IOException{
		
		double errorSum = 0;
		
		ObjectInputStream inIndex = null;
		ObjectInputStream inValue = null;
		ObjectOutputStream outValue = null;
		
		inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, true, type, false))));
		inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(n, false, type, false))));
		outValue = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(getLocalRPath(n, false, type, true))));
			
		int nnz = 0;
		if(type==CommonMapper.TYPE_TRAINING)
			nnz = nnzTraining[n];
		else if(type==CommonMapper.TYPE_TEST)
			nnz = nnzTest[n];
		else
			nnz = nnzQuery[n];

		for(int elem=0; elem<nnz; elem++){
			int[] index = new int[N];
			float r = 0;
			for(int _mode=0; _mode<N; _mode++){
				index[_mode] = inIndex.readInt();
			}
			r = inValue.readFloat();
			r = (r+oldBias[index[nBias]]) - curBias[index[nBias]];
			outValue.writeFloat(r);

			if(measureCost)
				errorSum += r*r;
		}
			
		inIndex.close();
		inValue.close();
		outValue.close();
		
		replace(getLocalRPath(n, false, type, false));
		
		return errorSum;
	}
	

	/**
	 * send updated parameters to other machines and receive updated parameters from other machines.
	 * braodcast {}_{m}a^{(n)}_{*k} and receive a^{(n)}_{*k}
	 * @param outIter	outer iteration number
	 * @param n	order of factor matrix
	 * @param context
	 * @param fs
	 * @throws IOException 
	 */
	protected void communicateBias(int outIter, int n, Context context, FileSystem fs) throws IOException{
		
		//write mine {}_{m}b^{(n)}_{*}
		Path outPath = new Path(getHDFSBiasParamPath(outIter, n, machineId, false));
		FSDataOutputStream out = fs.create(outPath);
		for(int i=startIndex[n]; i<endIndex[n]; i++){
			out.writeFloat(curBias[i]);
		}
		out.close();

		markBiasWrite(outIter, n, fs);

		//check if others write their file and read it
		boolean[] markReadComplete = new boolean[M];
		markReadComplete[machineId] = true;
		while(true){

			long requestTime = System.currentTimeMillis();
			FileStatus[] statusList = fs.listStatus(new Path(getHDFSBiasParamPath(outIter, n, true)));
			shuffle(statusList);

			for(FileStatus status : statusList){

				int _machineId = Integer.valueOf(status.getPath().getName());

				if(markReadComplete[_machineId])
					continue;
				else {
					FSDataInputStream in = null;
					try{
						in = fs.open(new Path(getHDFSBiasParamPath(outIter, n, _machineId, false)));
						for(int i=getStartIndex(n, _machineId); i<getStartIndex(n, _machineId+1); i++){
							curBias[i]=in.readFloat();
						}
					} catch(Exception e){
						System.out.println(e.getMessage());
						context.getCounter("Error", "err").increment(1);
						continue;
					} finally{
						try { in.close(); } catch (Exception e) {}
					}
					markReadComplete[_machineId]=true;
				}
			}

			boolean markAll = true;
			for(int _machineId=0; _machineId<M; _machineId++){
				if(!markReadComplete[_machineId]){
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
	 * write log in HDFS to denote the updated parameters ({}_{m}b^{(n)}_{*}) are uploaded
	 * @param outIter
	 * @param n
	 * @param fs
	 */
	protected void markBiasWrite(int outIter, int n, FileSystem fs){
		while(true){
			FSDataOutputStream out = null;
			try {
				out = fs.create(new Path(getHDFSBiasParamPath(outIter, n, machineId, true)));
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Mark write Failed");
				continue;
			} finally {
				try { out.close(); } catch(Exception e){ e.printStackTrace(); }; //accepted error
			}
			break;
		}
	}
	
	/**
	 * write log in HDFS to denote the updated parameters ({}_{m}a^{(n)}_{*k}) are uploaded
	 * @param outIter
	 * @param k	column
	 * @param inIter
	 * @param n	mode
	 * @param fs
	 */
	protected void markWrite(int outIter, int k, int inIter, int n, FileSystem fs){
		while(true){
			FSDataOutputStream out = null;
			try {
				out = fs.create(new Path(getHDFSParamPath(outIter, k, inIter, n, machineId, true)));
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Mark write Failed");
				continue;
			} finally {
				try { out.close(); } catch(Exception e){ e.printStackTrace(); }; //accepted error
			}
			break;
		}
	}
	
	/**
	 * write b^{(n)}
	 * @param n
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void writeBiasParams(int n) throws FileNotFoundException, IOException{
		ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalBiasParamPath(n, false))));
		for(int row=0; row<modeLengths[n]; row++){
			os.writeFloat(curBias[row]);
		}
		os.close();
	}
	
	/**
	 * load bias b^{(n)} from local disk
	 * @param n
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	protected void loadBiasFromLocal(int n) throws FileNotFoundException, IOException{	
		curBias = new float[modeLengths[n]];
		ObjectInputStream in = new ObjectInputStream(new BufferedInputStream (new FileInputStream(getLocalBiasParamPath(n, false))));
		for(int i=0; i<modeLengths[n]; i++){
			curBias[i]=in.readFloat();
		}
		in.close();
	}
	
	/**
	 * create initial factor matrix A^{(n)}
	 * @param n	mode
	 * @param k	column
	 * @param scale	maximum value
	 * @param context
	 * @throws IOException
	 */
	protected void createParamMatrix(int n, int k, float scale, Random random, Context context) throws IOException{
		for(int col=0; col<k; col++){
			context.progress();
			ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalParamPath(n, col, false))));
			for(int row=0; row<modeLengths[n]; row++){
				os.writeFloat(random.nextFloat()*scale);
			}
			os.close();
		}
	}
	
	/**
	 * create b^{(n)}
	 * @param n	mode
	 * @param context
	 * @throws IOException
	 */
	protected void createBiasTerms(int n, Context context) throws IOException{
		ObjectOutputStream os = new ObjectOutputStream(new BufferedOutputStream (new FileOutputStream(getLocalBiasParamPath(n, false))));
		for(int row=0; row<modeLengths[n]; row++){
			os.writeFloat(0.0f);
		}
		os.close();
	}
	
	/**
	 * get file path in local disk to save/load R entries {}_{m}R^{(n)}
	 * @param n	nide
	 * @param isIndex	true: the file is to save index / false: the file is to save entry value
	 * @param type	training / test / query
	 * @param isTemp	temporary file or not
	 * @return	file path
	 */
	protected String getLocalRPath(int n, boolean isIndex, int type, boolean isTemp){
		if(isIndex){
			if(type==CommonMapper.TYPE_TRAINING)
				return baseLocalPath+"/indexR"+n+(isTemp ? "_T" : "");
			else if(type==CommonMapper.TYPE_TEST)
				return baseLocalPath+"/indexRT"+n+(isTemp ? "_T" : "");
			else
				return baseLocalPath+"/indexRQ"+n+(isTemp ? "_T" : "");
		}
		else {
			if(type==CommonMapper.TYPE_TRAINING)
				return baseLocalPath+"/valueR"+n+(isTemp ? "_T" : "");
			else if(type==CommonMapper.TYPE_TEST)
				return baseLocalPath+"/valueRT"+n+(isTemp ? "_T" : "");
			else
				return baseLocalPath+"/valueRQ"+n+(isTemp ? "_T" : "");
		}
	}
	
	/**
	 * get directory path in local disk to save/load parameters of given factor matrix A^{(n)}
	 * @param n	mode
	 * @return directory path
	 */
	protected String getLocalParamPath(int n){
		return baseLocalPath+"/"+n;
	}
	
	/**
	 * get file path in local disk to save/load parameters in the kth column of the factor matrix A^{(n)} (i.e., a^{(n)}_{*k}}
	 * @param n	mode
	 * @param k	column
	 * @param isTemp	temporary file or not
	 * @return file path
	 */
	protected String getLocalParamPath(int n, int k, boolean isTemp){
		return getLocalParamPath(n)+"/"+k+(isTemp ? "_T" : "");
	}

	/**
	 * get file path in local disk to save/load b^{(n)}
	 * @param n	mode
	 * @param isTemp	temporary file or not
	 * @return file path
	 */
	protected String getLocalBiasParamPath(int n, boolean isTemp){
		return getLocalParamPath(n)+"/B"+(isTemp ? "_T" : "");
	}

	/**
	 * get directory path in HDFS where other machine saves updated parameters {}_{m}a^{(n)}_{*k}
	 * @param outIter	outer iteration number
	 * @param k	column
	 * @param inIter	inner iteration number
	 * @param n	mode
	 * @param log	log file or data file
	 * @return directory path
	 */
	protected String getHDFSParamPath(int outIter, int k, int inIter, int n, boolean log){
		String path = baseHDFSPath;
		if(log)
			path += "/log/";
		else
			path += "/data/";
		path += outIter+"/"+k+"/"+inIter+"/"+n;
		return path;
	}
	
	/**
	 * get file path in HDFS where other machine saves updated parameters {}_{m}a^{(n)}_{*k}
	 * @param outIter	outer iteration number
	 * @param k	column
	 * @param inIter	inner iteration number
	 * @param n	mode
	 * @param m	machine index
	 * @param log	whether log (true) or data (false)
	 * @return file path
	 */ 
	protected String getHDFSParamPath(int outIter, int k, int inIter, int n, int m, boolean log){
		return getHDFSParamPath(outIter, k, inIter, n, log)+"/"+m;
	}
	
	/**
	 * get directory path in HDFS where other machine saves updated parameters {}_{m}b^{(n)}
	 * @param outIter	outer iteration number
	 * @param n	mode
	 * @param log	log (true) or data (false)
	 * @return directory path
	 */
	protected String getHDFSBiasParamPath(int outIter, int n, boolean log){
		
		String path = baseHDFSPath;
		if(log)
			path += "/log/";
		else
			path += "/data/";
		path += outIter+"/B/"+n;
		return path;
	}
	
	/**
	 * get file path in HDFS where other machine saves updated parameters {}_{m}a^{(n)}_{*k}
	 * @param outIter	outer iteration number
	 * @param n	mode
	 * @param m	machine index
	 * @param log	log (true) or data (false)
	 * @return file path
	 */
	protected String getHDFSBiasParamPath(int outIter, int n, int m, boolean log){
		return getHDFSBiasParamPath(outIter, n, log)+"/"+m;
	}
	
	/**
	 * first row of the factor matrix A^{(n)} assigned to machine m
	 * @param n	order of factor matrix
	 * @param m	machine index
	 * @return	start index
	 */
	protected int getStartIndex(int n, int m){
		return (int)Math.ceil(m*modeLengths[n]/(0.0+M));
	}
	
	/**
	 * replace a new file with an old file
	 * @param path	path of a file to replace
	 */
	protected void replace(String path){
		File oriFile = new File(path);
		boolean success = oriFile.delete();
		if(!success){
			System.out.println(path+" delete Failed");
		}
		File newFile = new File(path+"_T");
		success  = newFile.renameTo(new File(path));
		if(!success){
			System.out.println(path+" replace Failed");
		}
	}
	
	/**
	 * shuffle fileList for load balancing
	 * @param status
	 */
	protected static void shuffle(FileStatus[] status){
		int n = status.length;
		Random random = new Random();
	    for (int i = 0; i < n; i++){
	    	int randI = random.nextInt(n-i);
	    	FileStatus temp = status[n-i-1];
	    	status[n-i-1] = status[randI];
	    	status[randI] = temp;
	    }
	}
	
	/**
	 * Output bias terms
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeBiasesResults(Context context) throws IOException, InterruptedException{

		int[][] invertedIdx = GreedyRowAssignment.getInvertedIndex(N, modeLengths, conf);
		for(int n=0; n<N; n++){
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempLocalFile));
			ObjectInputStream br = new ObjectInputStream(new FileInputStream(getLocalBiasParamPath(n, false)));
			
			for(int i=0; i<modeLengths[n]; i++){
				StringBuffer buffer = new StringBuffer();
				buffer.append(invertedIdx[n][i]);
				buffer.append(",");
				buffer.append(br.readFloat());
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			
			br.close();
			bw.close();
			
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(true, new Path(tempLocalFile), new Path(conf.get(P_BIASES, "")+"/"+(n+1)));
			fs.close();
		}
	}
	
	/**
	 * Write performance statistics in HDFS
	 * @param context
	 * @param result
	 * @throws IOException
	 */
	protected void writePerformance(Context context, double[][] result) throws IOException{
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(tempLocalFile));
		for(int outIter = 0; outIter<Tout; outIter++){	
			bw.write(String.format("%d,%d,%f,%d,%f,%d\n", (int)result[outIter][0]+1, (int)result[outIter][1], 
				result[outIter][2], (int)result[outIter][3], result[outIter][4], (int)result[outIter][5]));
		}
		bw.close();
		
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(true, new Path(tempLocalFile), new Path(conf.get(P_PERFORMANCE_TEMP, "")+"/"+machineId));
		fs.close();
		
	}
	
	/**
	 * Write the estimated value of each query entry 
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeEstimate(Context context) throws IOException, InterruptedException{

		//write Query
		int[][] invertedIdx = GreedyRowAssignment.getInvertedIndex(N, modeLengths, conf);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(tempLocalFile));
		ObjectInputStream inIndex = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(0, true, CommonMapper.TYPE_QUERY, false))));
		ObjectInputStream inValue = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getLocalRPath(0, false, CommonMapper.TYPE_QUERY, false))));
			
		int nnz = nnzQuery[0];
			
		for(int elem=0; elem<nnz; elem++){
			
			StringBuffer buffer = new StringBuffer();
			
			int[] index = new int[N];
			for(int _mode=0; _mode<N; _mode++){
				index[_mode] = invertedIdx[_mode][inIndex.readInt()];
				buffer.append(index[_mode]+",");
			}
			buffer.append(""+(-1*inValue.readFloat()));
			bw.write(buffer.toString()+"\n");
		}
			
		inIndex.close();
		inValue.close();
		bw.close();
		
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(true, new Path(tempLocalFile), new Path(conf.get(P_ESTIMATE, "")+"/"+machineId));
		fs.close();
		
	}
	
	/**
	 * Output factor matrices
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected void writeFactormatricesResult(Context context) throws IOException, InterruptedException{

		int[][] invertedIdx = GreedyRowAssignment.getInvertedIndex(N, modeLengths, conf);
		for(int n=0; n<N; n++){
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempLocalFile));
			ObjectInputStream[] brs = new ObjectInputStream[K];
			
			for(int k=0; k<K; k++){
				brs[k] = new ObjectInputStream(new FileInputStream(getLocalParamPath(n, k, false)));
			}
			
			for(int i=0; i<modeLengths[n]; i++){
				StringBuffer buffer = new StringBuffer();
				buffer.append(invertedIdx[n][i]);
				for(int k=0; k<K; k++){
					buffer.append(",");
					buffer.append(brs[k].readFloat());
				}
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			
			for(int k=0; k<K; k++){
				brs[k].close();
			}
			
			bw.close();
			
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(true, new Path(tempLocalFile), new Path(conf.get(P_FACTORMATRICES, "")+"/"+(n+1)));
			fs.close();
		}
	}
	
	
	
}