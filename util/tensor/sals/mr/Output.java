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

import static util.tensor.sals.mr.Params.P_AVERAGE_PATH;
import static util.tensor.sals.mr.Params.P_BIASES;
import static util.tensor.sals.mr.Params.P_CARDINALITY;
import static util.tensor.sals.mr.Params.P_ESTIMATE;
import static util.tensor.sals.mr.Params.P_FACTORMATRICES;
import static util.tensor.sals.mr.Params.P_INDEXMAP;
import static util.tensor.sals.mr.Params.P_M;
import static util.tensor.sals.mr.Params.P_PERFORMANCE;
import static util.tensor.sals.mr.Params.P_PERFORMANCE_TEMP;
import static util.tensor.sals.mr.Params.P_TOUT;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

/**
 * Common functions related to output
 * <P>
 * @author Kijung
 */
public class Output {

	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * set path of output files
	 * @param outputDir path of directory to save output files
	 * @param conf
	 */
	public static void setOutputPath(String outputDir, Configuration conf){
		conf.set(P_CARDINALITY, outputDir+"/cardinality");
		conf.set(P_INDEXMAP, outputDir+"/indexmap");
		conf.set(P_ESTIMATE, outputDir+"/estimate.out");
		conf.set(P_AVERAGE_PATH, outputDir+"/mu");
		conf.set(P_PERFORMANCE, outputDir+"/performance.out");
		conf.set(P_PERFORMANCE_TEMP, outputDir+"/performance.temp");
		conf.set(P_FACTORMATRICES, outputDir+"/factor_matrices");
		conf.set(P_BIASES, outputDir+"/bias_terms");
	}
	
	/**
	 * Aggregate the performance statistics of reducers 
	 * @param job
	 * @param conf
	 */
	public static void performanceOutput(Job job, Configuration conf){
		
		int M = conf.getInt(P_M, 1);
		int Tout = conf.getInt(P_TOUT,0);
		
		double[][] result = new double[Tout][6];
		
		String userHome = System.getProperty("user.home");
		String tempFile = userHome+"/PEGASUS_TEMP_"+job.getJobID();
		String tempFile2 = userHome+"/PEGASUS_TEMP_2_"+job.getJobID();
		
		FileSystem fs = null;
		BufferedReader br = null;
		BufferedWriter bw = null;
		try{
			fs = FileSystem.get(conf);
			
			for(int m=0; m<M; m++){
				
				fs.copyToLocalFile(new Path(conf.get(P_PERFORMANCE_TEMP)+"/"+m), new Path(tempFile));
				br = new BufferedReader(new FileReader(tempFile));
				
				for(int outIter=0; outIter<Tout; outIter++){
					String line = br.readLine();
					String[] tokens = line.split(",");
					result[outIter][0] = Integer.valueOf(tokens[0]);
					result[outIter][1] = Math.max(Integer.valueOf(tokens[1]), result[outIter][1]);
					result[outIter][2] += Double.valueOf(tokens[2]);
					result[outIter][3] += Double.valueOf(tokens[3]);
					result[outIter][4] += Double.valueOf(tokens[4]);
					result[outIter][5] += Double.valueOf(tokens[5]);
				}
				
				br.close();
			}
			
			new File(tempFile).delete();
			
			bw = new BufferedWriter(new FileWriter(tempFile2));
			
			String line = "iteration,elapsed_time,training_rmse,test_rmse\n";
			System.out.print(line);
			bw.write(line);
			for(int outIter=0; outIter<Tout; outIter++){
				line = String.format("%d,%d,%f,%f\n", (int)result[outIter][0], (int)result[outIter][1], Math.sqrt(result[outIter][2]/result[outIter][3]), result[outIter][5]==0 ? 0.0f : Math.sqrt(result[outIter][4]/result[outIter][5]));
				System.out.print(line);
				bw.write(line);
			}
			bw.close();
			
			fs.copyFromLocalFile(true, new Path(tempFile2), new Path(conf.get(P_PERFORMANCE)));
			
		} catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {fs.close();} catch(Exception e){};
		}
		
	}
	
}
