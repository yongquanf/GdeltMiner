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
import static util.tensor.sals.mr.Params.P_DELIM;
import static util.tensor.sals.mr.Params.P_INDEX_VALUE;
import static util.tensor.sals.mr.Params.P_TRAINING;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Calculate the average of the entries of training data
 * <P>
 * @author Kijung
 */
public class Average {
	
	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * calculate the average of the entries of training data
	 * @param conf
	 * @return the average of the entries of training data
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static float run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		/*
		 * run average job
		 */
		Job job = new Job(conf, "AVERAGE");
		job.setJarByClass(Average.class);
		job.setMapperClass(Average.AverageMapper.class);
		job.setReducerClass(Average.AverageReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class); 
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(conf.get(P_TRAINING, "")));
		
		job.waitForCompletion(true);
		
		
		/*
		 * read output
		 */
		String userHome = System.getProperty("user.home");
		String tempFile = userHome+"/PEGASUS_TEMP_"+job.getJobID()+"_"+System.currentTimeMillis();
		
		FileSystem fs = FileSystem.get(conf);
		fs.copyToLocalFile(true, new Path(conf.get(P_AVERAGE_PATH)+"_obj"), new Path(tempFile));
		
		float average = 0;
		ObjectInputStream os = new ObjectInputStream(new FileInputStream(tempFile));
		average = os.readFloat();
		os.close();
		fs.close();
		
		new File(tempFile).delete();
		
		return average;
		
	}
	
	/**
	 * count the number of entries and caculate their sums
	 */
	public static class AverageMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private String delim;
		private int valueIdx;
		private int count = 0; // number of entries
		private double sum = 0; // sum of entries

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * load parameters
		 */
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			delim = conf.get(P_DELIM," ");
			valueIdx = conf.getInt(P_INDEX_VALUE, 0);
		}

		/**
		 * update sum and count
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(delim);
			sum += Double.valueOf(tokens[valueIdx]);
			count++;
		}
		
		/**
		 * write sum and count
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			context.write(new IntWritable(count), new DoubleWritable(sum));
		}
	}


	/**
	 * aggregate the result from mappers
	 */
	public static class AverageReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, NullWritable> {

		////////////////////////////////////
		// private methods
		////////////////////////////////////
		
		private int count = 0; // number of entries
		private double sum = 0; // sum of entries

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * update sum and count
		 */
		@Override
		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			count += key.get();
			for(DoubleWritable value: values){
				sum += value.get();
			}
		}
		
		/**
		 * write output
		 */
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
			
			String userHome = System.getProperty("user.home");
			String tempFile = userHome+"/PEGASUS_TEMP_"+context.getJobID()+"_"+System.currentTimeMillis();
			String tempFileObj = userHome+"/PEGASUS_TEMP2_"+context.getJobID()+"_"+System.currentTimeMillis();
			
			/*
			 * write in text format
			 */
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
			bw.write(new BigDecimal(sum/count).toString());
			bw.close();
			
			/*
			 * write in the object format without lossing accuracy  
			 */
			ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(tempFileObj));
			os.writeFloat((float) (sum/count));
			os.close();
			
			/*
			 * upload to HDFS
			 */
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(true, new Path(tempFile), new Path(conf.get(P_AVERAGE_PATH)));
			fs.copyFromLocalFile(true, new Path(tempFileObj), new Path(conf.get(P_AVERAGE_PATH)+"_obj"));
			fs.close();
			
		}

	}

}
