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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

/**
 * Common functions related to output
 * <P>
 * @author Kijung
 */
public class Output {

	////////////////////////////////////
	//public methods
	////////////////////////////////////
	
	/**
	 * calculate estimated values for query data
	 * @param queryTensor	query data
	 * @param params	factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @param N	dimension
	 * @param K	rank
	 */
	public static void calculateEstimate(Tensor queryTensor, float[][][] params, int N, int K){
		for(int elemIdx =0; elemIdx < queryTensor.omega; elemIdx++){
			float predict = 0;
			for(int k=0; k<K; k++){
				float product = 1;
				for(int n=0; n<N; n++){
					product *= params[n][queryTensor.indices[n][elemIdx]][k];
				}
				predict += product;
			}
			queryTensor.values[elemIdx] = predict;
		}
	}
	
	/**
	 * calculate estimated values for query data
	 * @param queryTensor	query data
	 * @param mu	the average of the entries of the training matrix
	 * @param bias	bias terms (n, i_{n}) -> b^{(n)}_{i_{n}}
	 * @param params	factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @param N	dimension
	 * @param K	rank
	 */
	public static void calculateEstimate(Tensor queryTensor, float mu, float[][] bias, float[][][] params, int N, int K){
		for(int elemIdx =0; elemIdx < queryTensor.omega; elemIdx++){
			float predict = mu;
			for(int n=0; n<N; n++){
				predict += bias[n][queryTensor.indices[n][elemIdx]];
			}
			for(int k=0; k<K; k++){
				float product = 1;
				for(int n=0; n<N; n++){
					product *= params[n][queryTensor.indices[n][elemIdx]][k];
				}
				predict += product;
			}
			queryTensor.values[elemIdx] = predict;
		}
	}
	
	/**
	 * write estimated values to a given output directory
	 * @param outputDir	directory to write output files
	 * @param query	query data
	 * @param N	dimension
	 * @throws IOException
	 */
	public static void writeEstimate(String outputDir, Tensor query, int N) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+"estimate.out"));
		for(int elemIdx =0; elemIdx < query.omega; elemIdx++){
			for(int dim=0; dim<N; dim++){
				bw.write(query.indices[dim][elemIdx]+",");
			}
			bw.write(query.values[elemIdx]+"\n");
		}
		bw.close();
	}
	
	/**
	 * write estimated values to a given output directory
	 * @param outputDir	directory to write output files
	 * @param query	query data
	 * @param permutedIdx	row permutation
	 * @param N	dimension
	 * @throws IOException
	 */
	public static void writeEstimate(String outputDir, Tensor query, int[][] permutedIdx, int N) throws IOException{
		int[][] invertedIdx = new int[N][];
		for(int dim=0; dim<N; dim++){
			invertedIdx[dim] = new int[permutedIdx[dim].length];
			for(int i=0; i<permutedIdx[dim].length; i++){
				invertedIdx[dim][permutedIdx[dim][i]] = i;
			}
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+"estimate.out"));
		for(int elemIdx =0; elemIdx < query.omega; elemIdx++){
			for(int dim=0; dim<N; dim++){
				bw.write(invertedIdx[dim][query.indices[dim][elemIdx]]+",");
			}
			bw.write(query.values[elemIdx]+"\n");
		}
		bw.close();
	}
	
	/**
	 * write performance summary to a given output directory
	 * @param outputDir	directory to write output files
	 * @param performance	performance results
	 * @param Tout	number of outer iterations
	 */
	public static void writePerformance(String outputDir, double[][] performance, int Tout) throws IOException{
		
		if(!FileUtils.getFile(outputDir).isDirectory()) {
			FileUtils.getFile(outputDir).mkdir();
		}
		
		
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+"performance.out"));
		bw.write("iteration,elapsed_time,training_rmse,test_rmse");
		for(int outIter = 0; outIter<Tout; outIter++){
			bw.write(String.format("%d,%d,%f,%f\n", (int)performance[outIter][0], (int)performance[outIter][1], performance[outIter][2], performance[outIter][3]));
		}
		bw.close();
	}
	
	/**
	 * write performance summary to a given output directory
	 * @param outputDir	directory to write output files
	 * @param performance	performance results
	 * @param Tout	number of outer iterations
	 */
	public static void writePerformanceTimeOnly(String outputDir, double[][] performance, int Tout) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+"performance.out"));
		bw.write("iteration,elapsed_time");
		for(int outIter = 0; outIter<Tout; outIter++){
			bw.write(String.format("%d,%d\n", (int)performance[outIter][0], (int)performance[outIter][1]));
		}
		bw.close();
	}
	
	
	/**
	 * write factor matrices to a given output directory
	 * @param outputDir	directory to write output files
	 * @param params	factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @throws IOException
	 */
	public static void writeFactorMatrices(String outputDir, float[][][] params) throws IOException{
		
		outputDir = outputDir+File.separator+"factor_matrices";
		
		int N = params.length; //dimension
		
		File file = new File(outputDir);
		if(file.exists())
			file.delete();
		file.mkdir();

		for(int n=0; n<N; n++){
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+(n+1)));
			int modeLength = params[n].length;
			int K = params[n][0].length; //rank
			for(int i=0; i<modeLength; i++){
				StringBuffer buffer = new StringBuffer();
				buffer.append(i);
				buffer.append(",");
				for(int k=0; k<K-1; k++){
					buffer.append(params[n][i][k]);
					buffer.append(",");
				}
				buffer.append(params[n][i][K-1]);
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			bw.close();
		}
	}
	
	/**
	 * write factor matrices to a given output directory
	 * @param outputDir	directory to write output files
	 * @param params	factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * @param permutedIdx	row permutation
	 * @throws IOException
	 */
	public static void writeFactorMatrices(String outputDir, float[][][] params, int[][] permutedIdx) throws IOException{
		
		outputDir = outputDir+File.separator+"factor_matrices";
		
		int N = params.length; //dimension
		
		File file = new File(outputDir);
		if(file.exists())
			file.delete();
		file.mkdir();

		for(int n=0; n<N; n++){
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+(n+1)));
			int modeLength = params[n].length;
			int K = params[n][0].length; //rank
			for(int i=0; i<modeLength; i++){
				StringBuffer buffer = new StringBuffer();
				int rowIndex = permutedIdx[n][i];
				buffer.append(i);
				buffer.append(",");
				for(int k=0; k<K-1; k++){
					buffer.append(params[n][rowIndex][k]);
					buffer.append(",");
				}
				buffer.append(params[n][rowIndex][K-1]);
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			
			bw.close();
		}
	}
	
	/**
	 * write bias terms to a given output directory
	 * @param outputDir	directory to write output files
	 * @param bias	bias terms (n, i_{n}) -> b^{(n)}_{i_{n}}
	 * @throws IOException
	 */
	public static void writeBiases(String outputDir, float[][] bias) throws IOException{
		
		outputDir = outputDir+File.separator+"bias_terms";
		
		int N = bias.length; //dimension
		
		File file = new File(outputDir);
		if(file.exists())
			file.delete();
		file.mkdir();

		for(int n=0; n<N; n++){
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+(n+1)));
			int modeLength = bias[n].length;
			for(int i=0; i<modeLength; i++){
				StringBuffer buffer = new StringBuffer();
				buffer.append(i);
				buffer.append(",");
				buffer.append(bias[n][i]);
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			
			bw.close();
		}
	}
	
	/**
	 * write bias terms to a given output directory
	 * @param outputDir	directory to write output files
	 * @param bias	bias terms (n, i_{n}) -> b^{(n)}_{i_{n}}
	 * @param permutedIdx	row permutation
	 * @throws IOException
	 */
	public static void writeBiases(String outputDir, float[][] bias, int[][] permutedIdx) throws IOException{
		
		outputDir = outputDir+File.separator+"bias_terms";
		
		int N = bias.length; //dimension
		
		File file = new File(outputDir);
		if(file.exists())
			file.delete();
		file.mkdir();

		for(int n=0; n<N; n++){
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir+File.separator+(n+1)));
			int modeLength = bias[n].length;
			for(int i=0; i<modeLength; i++){
				StringBuffer buffer = new StringBuffer();
				int rowIndex = permutedIdx[n][i];
				buffer.append(i);
				buffer.append(",");
				buffer.append(bias[n][rowIndex]);
				buffer.append("\n");
				bw.write(buffer.toString());
			}
			
			bw.close();
		}
	}
	
	/**
	 * write mu (the average of the entries of training data)
	 * @param outputDir	directory to write output files
	 * @param mu
	 * @throws IOException
	 */
	public static void writeMU(String outputDir, float mu) throws IOException{
		outputDir = outputDir+File.separator+"mu";
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputDir));
		bw.write(""+mu);
		bw.write("\n");
		bw.close();
	}

}
