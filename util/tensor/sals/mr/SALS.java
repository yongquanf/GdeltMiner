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

import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop version of SALS
 * <P>
 * @author Kijung
 */
public class SALS extends Configured implements Tool {
	
	////////////////////////////////////
	// private fields
	////////////////////////////////////
	
	private boolean useBias = false; //whether to use bias terms

	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * @param useBias	whether to use bias terms
	 */
	public SALS(boolean useBias){
		this.useBias = useBias;
	}
	
	/**
	 * main function to run ALS
	 * @param args	[training] [output] [M] [Tout] [Tin] [N] [K] [C] [useWeight] [lambda] [I1] [I2] ... [IN] [memory] [test] [query]
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SALS(false), args);
		System.exit(exitCode); 
	}
	
	/**
	 * run SALS
	 * @param args
	 * @throws Exception
	 */
	public int run (String[] args) throws Exception {
		
		/*
		 * parameter check
		 */
		Configuration conf = getConf();
		
		conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 1);
		conf.set(P_DELIM, ",");
		conf.setInt(P_WAITING, 100);
		conf.setInt(P_SEED, new Random().nextInt());
		
		System.out.println("========================");
		System.out.println("Start parameter check...");
		System.out.println("========================");
		
		String training = args[0];
		System.out.println("-"+P_TRAINING+": "+training);
		conf.set(P_TRAINING, training);
		
		String outputDir = args[1];
		System.out.println("-"+P_OUTPUT+": "+outputDir);
		conf.set(P_OUTPUT, outputDir);
		
		int M = Integer.valueOf(args[2]);
		System.out.println("-"+P_M+": "+M);
		conf.setInt(P_M, M);
		
		int Tout = Integer.valueOf(args[3]);
		System.out.println("-"+P_TOUT+": "+Tout);
		conf.setInt(P_TOUT, Tout);
		
		int Tin = Integer.valueOf(args[4]);
		System.out.println("-"+P_TIN+": "+Tin);
		conf.setInt(P_TIN, Tin);
		
		int N = Integer.valueOf(args[5]);
		System.out.println("-"+P_N+": "+N);
		conf.setInt(P_N, N);
		
		int K = Integer.valueOf(args[6]);
		System.out.println("-"+P_K+": "+K);
		conf.setInt(P_K, K);
		
		int C = Integer.valueOf(args[7]);
		System.out.println("-"+P_C+": "+C);
		conf.setInt(P_C, C);

		boolean useWeight = Integer.valueOf(args[8]) > 0;
		System.out.println("-useWeight: "+useWeight);
		conf.setBoolean(P_USE_WEIGHT, useWeight);

		float lambda = Float.valueOf(args[9]);
		System.out.println("-"+P_LAMBDA+": "+lambda);
		conf.setFloat(P_LAMBDA, lambda);

        float lambdaForBiases = 0;
        int base = 10;
        if(useBias) {
            lambdaForBiases = Float.valueOf(args[base].trim());
            conf.setFloat(P_LAMBDA_BIAS, lambda);
            System.out.println("-lambdaForBiases: "+lambdaForBiases);
            base++;
        }
		
		for(int dim=0; dim<N; dim++){
			int modeLength = Integer.valueOf(args[base+dim]);
			System.out.println("-"+P_MODELENGTH+(dim+1)+": "+modeLength);
			conf.setInt(P_MODELENGTH+(dim+1), modeLength);
			conf.setInt(P_INDEX+(dim+1), dim);
		}
		conf.setInt(P_INDEX_VALUE, N);
		
		String memory = "-Xmx"+Integer.valueOf(args[base+N])+"m";
		System.out.println("-memory: "+memory);
		conf.set(P_MEMORY, memory);
		
		String test = null;
		if(args.length > base+N+1 ){
			test = args[base+N+1];
			System.out.println("-"+P_TEST+": "+test);
			conf.set(P_TEST, test);
		}
		String query = null;
		if(args.length > base+N+2 ){
			query = args[base+N+2];
			System.out.println("-"+P_QUERY+": "+query);
			conf.set(P_QUERY, query);
		}
		
		Output.setOutputPath(outputDir, conf);
		
		/*
		 * init HDFS
		 */
		initHDFS(conf);
		
		/*
		 * compute average
		 */
		if(useBias){
			System.out.println("==============================");
			System.out.println("Calculate average...");
			System.out.println("==============================");
			
			float average = Average.run(conf);
			conf.setFloat(P_AVERAGE, average);
			conf.setBoolean(P_USE_BIAS, true);
		}
		
		/*
		 * run greedy row assignment
		 */
		System.out.println("==============================");
		System.out.println("Start greedy row assignment...");
		System.out.println("==============================");
		
		GreedyRowAssignment.run(conf);

		String method = "";
		if(useBias)
			method="Bias-SALS";
		else
			method="SALS";
		
		/*
		 * run SALS
		 */
		System.out.println("=============");
		System.out.println("Start "+method+"...");
		System.out.println("=============");
		System.out.println("The main process of "+method+" runs at cleanup stage which follows after reduce stage.");
		System.out.println("So, it may take several minutes (or hours depending on the data size) after reduce stage finishes.");	
		
		DistributedCache.addCacheFile(new URI(conf.get(P_INDEXMAP)), conf); // share the result of greedy row assignment using distributed cache
		
		Job job = new Job(conf, method);
		
		job.setJarByClass(SALS.class);
		job.setMapperClass(CommonMapper.class); 
		job.setReducerClass(SALSReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);

		job.setMapOutputKeyClass(TripleWritable.class); 
		job.setMapOutputValueClass(ElementWritable.class);

		job.setPartitionerClass(CommonPartitioner.class);
		
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(conf.getInt(P_M, 1));

		MultipleInputs.addInputPath(job, new Path(conf.get(P_TRAINING, "")), TextInputFormat.class, CommonMapper.TrainingMapper.class); 
		
		if(conf.get(P_TEST)!=null){
			MultipleInputs.addInputPath(job, new Path(conf.get(P_TEST, "")), TextInputFormat.class, CommonMapper.TestMapper.class);
		}
		if(conf.get(P_QUERY)!=null){
			MultipleInputs.addInputPath(job, new Path(conf.get(P_QUERY, "")), TextInputFormat.class, CommonMapper.QueryMapper.class);
		}
		
		
		job.waitForCompletion(true);
		
		/*
		 * write outputs
		 */
		System.out.println("=======================");
		System.out.println("Start writing output...");
		System.out.println("=======================");
		
		Output.performanceOutput(job, conf);

		return 0;
	}
	
	////////////////////////////////////
	// private methods
	////////////////////////////////////
	
	/**
	 * initialize a HDFS directory to save temporary data and output
	 * @param conf
	 * @throws Exception 
	 */
	private void initHDFS(Configuration conf) throws Exception{
		
		int dimension = conf.getInt(P_N, 0);
		int Tout = conf.getInt(P_TOUT,0);
		int K = conf.getInt(P_K, 0);
		int C = conf.getInt(P_C, 0);
		int Tin = conf.getInt(P_TIN,0);

		String baseStr = conf.get(P_OUTPUT, "");
		String logStr = baseStr+"/log";
		String dataStr = baseStr+"/data";

		FileSystem fs = null;

		fs = FileSystem.get(conf);

		Path basePath = new Path(baseStr);
		if(fs.exists(basePath)){
			fs.delete(basePath, true);
		}
		fs.mkdirs(basePath);
		fs.mkdirs(new Path(logStr));
		fs.mkdirs(new Path(dataStr));
		fs.mkdirs(new Path(conf.get(P_PERFORMANCE_TEMP, "")));
		fs.mkdirs(new Path(conf.get(P_ESTIMATE, "")));
		if(conf.getBoolean(P_USE_BIAS, false)){
			fs.mkdirs(new Path(conf.get(P_BIASES, "")));
		}

		for(int outIter=0; outIter<Tout; outIter++){

			fs.mkdirs(new Path(dataStr+"/"+outIter));
			fs.mkdirs(new Path(logStr+"/"+outIter));

			if(conf.getBoolean(P_USE_BIAS, false)){

				fs.mkdirs(new Path(dataStr+"/"+outIter+"/B"));
				fs.mkdirs(new Path(logStr+"/"+outIter+"/B"));

				for(int dim=0; dim<dimension; dim++){
					fs.mkdirs(new Path(dataStr+"/"+outIter+"/B/"+dim));
					fs.mkdirs(new Path(logStr+"/"+outIter+"/B/"+dim));
				}
			}

			for(int splitIter=0; splitIter<Math.ceil((K+0.0)/C); splitIter++){

				fs.mkdirs(new Path(dataStr+"/"+outIter+"/"+splitIter));
				fs.mkdirs(new Path(logStr+"/"+outIter+"/"+splitIter));

				for(int inIter=-1; inIter<Tin; inIter++){

					fs.mkdirs(new Path(dataStr+"/"+outIter+"/"+splitIter+"/"+inIter));
					fs.mkdirs(new Path(logStr+"/"+outIter+"/"+splitIter+"/"+inIter));

					for(int dim=0; dim<dimension; dim++){
						fs.mkdirs(new Path(dataStr+"/"+outIter+"/"+splitIter+"/"+inIter+"/"+dim));
						fs.mkdirs(new Path(logStr+"/"+outIter+"/"+splitIter+"/"+inIter+"/"+dim));
					}
				}
			}
		}

		fs.close();
	}
	
}
