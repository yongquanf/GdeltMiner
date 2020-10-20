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

import static util.tensor.sals.mr.Params.P_CARDINALITY;
import static util.tensor.sals.mr.Params.P_DELIM;
import static util.tensor.sals.mr.Params.P_INDEX;
import static util.tensor.sals.mr.Params.P_INDEXMAP;
import static util.tensor.sals.mr.Params.P_M;
import static util.tensor.sals.mr.Params.P_MODELENGTH;
import static util.tensor.sals.mr.Params.P_N;
import static util.tensor.sals.mr.Params.P_TRAINING;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import util.tensor.sals.single.ArrayMethods;

/**
 * Greedy row assignment of Hadoop version algorithms
 * <P>
 * @author Kijung
 */
public class GreedyRowAssignment {

	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	public static void run(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		/*
		 * The first step of greedy row assignment.
		 * Calculate the number of observable entries in each row.
		 */
		
		Job greedyJob1 = new Job(conf, "GREEDY_ROW_ASSIGNMENT-1");
		greedyJob1.setJarByClass(GreedyRowAssignment.class);
		greedyJob1.setMapperClass(GreedyRowAssignment.GreedyMapper.class);
		greedyJob1.setCombinerClass(GreedyRowAssignment.GreedyCombiner.class);
		greedyJob1.setReducerClass(GreedyRowAssignment.GreedyReducer.class);
		
		greedyJob1.setInputFormatClass(TextInputFormat.class);
		greedyJob1.setOutputFormatClass(TextOutputFormat.class);
		
		greedyJob1.setMapOutputKeyClass(PairWritable.class); 
		greedyJob1.setMapOutputValueClass(IntWritable.class);
		greedyJob1.setOutputKeyClass(PairWritable.class); 
		greedyJob1.setOutputValueClass(IntWritable.class);
		
		greedyJob1.setNumReduceTasks(conf.getInt(P_M, 1));
		
		FileInputFormat.addInputPath(greedyJob1, new Path(conf.get(P_TRAINING, "")));
		FileOutputFormat.setOutputPath(greedyJob1, new Path(conf.get(P_CARDINALITY, "")));
		
		greedyJob1.waitForCompletion(true);
		
		/*
		 * The second step of greedy row assignment.
		 * assign rows so that observable entries are split to machines equally.
		 */
		
		Job greedyJob2 = new Job(conf, "GREEDY_ROW_ASSIGNMENT-2");
		greedyJob2.setJarByClass(GreedyRowAssignment.class);
		greedyJob2.setMapperClass(GreedyRowAssignment.Greedy2Mapper.class);
		greedyJob2.setReducerClass(GreedyRowAssignment.Greedy2Reducer.class);
		
		greedyJob2.setInputFormatClass(TextInputFormat.class);
		greedyJob2.setOutputFormatClass(NullOutputFormat.class);
		
		greedyJob2.setMapOutputKeyClass(PairWritable.class); 
		greedyJob2.setMapOutputValueClass(IntWritable.class);
		
		greedyJob2.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(greedyJob2, new Path(conf.get(P_CARDINALITY, "")));
		
		greedyJob2.waitForCompletion(true);
	}
	
	/**
	 * get row permutation
	 * @param N	dimension
	 * @param modeLengths
	 * @param conf
	 * @throws IOException
	 * @return permuted index (n, old_index) -> new_index
	 */
	public static int[][] getPermutedIndex(int N, int[] modeLengths, Configuration conf) throws IOException{
		int[][] permutedIdx = new int[N][];
		for(int i=0; i<N; i++){
			permutedIdx[i] = new int[modeLengths[i]];
		}
			
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		FileInputStream fstream = new FileInputStream(localFiles[0].toString());
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		while(true){
			String line = br.readLine();
			if(line==null)
				break;
			String[] tokens = line.trim().split(",");
			permutedIdx[Integer.valueOf(tokens[0])][Integer.valueOf(tokens[1])] = Integer.valueOf(tokens[2]);
		}
		
		br.close();
		
		return permutedIdx;
	}
	
	/**
	 * get inverted index of greedy row assignment
	 * @param N	dimension
	 * @param modeLengths
	 * @param conf
	 * @throws IOException
	 * @return inverted index (n, new_index) -> old_index
	 */
	public static int[][] getInvertedIndex(int N, int[] modeLengths, Configuration conf) throws IOException{
		int[][] invertedIdx = new int[N][];
		for(int i=0; i<N; i++){
			invertedIdx[i] = new int[modeLengths[i]];
		}
			
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		FileInputStream fstream = new FileInputStream(localFiles[0].toString());
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		while(true){
			String line = br.readLine();
			if(line==null)
				break;
			String[] tokens = line.trim().split(",");
			invertedIdx[Integer.valueOf(tokens[0])][Integer.valueOf(tokens[2])] = Integer.valueOf(tokens[1]);
		}
		
		br.close();
		
		return invertedIdx;
	}
	
	
	/**
	 * mapper of the first step of greedy row assignment: calculating (n, i_{n}) -> |\Omega^{(n)}_{i_{n}}|
	 */
	public static class GreedyMapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private String delim; // delimeter
		private int[] modeIdx; // column index of each mode input files
		private int N; //dimension

		private PairWritable keyWritable; // (mode, row index)
		private IntWritable intWritable; // number of entries

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * load parameters
		 */
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			N = conf.getInt(P_N, 0);

			ElementWritable.N = N;
			keyWritable = new PairWritable();
			intWritable = new IntWritable(1);

			modeIdx = new int[N];

			delim = conf.get(P_DELIM," ");
			for(int i=0; i<N; i++){
				modeIdx[i] = conf.getInt(P_INDEX+(i+1), 0);
			}
		}

		/**
		 * output (<n, i_{n}>, 1)
		 * @param key line number
		 * @param value line
		 * @param context
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(delim);
			for(int dim=0; dim<N; dim++){
				int index = Integer.valueOf(tokens[modeIdx[dim]]);
				keyWritable.set(dim, index);
				context.write(keyWritable, intWritable);
			}
		}
	}

	/**
	 * combiner of the first step of greedy row assignment: calculating (n, i_{n}) -> |\Omega^{(n)}_{i_{n}}|
	 */
	public static class GreedyCombiner extends Reducer<PairWritable, IntWritable, PairWritable, IntWritable> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private IntWritable intWritable = new IntWritable(0); // number of entries

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * aggregate the result of each machine.
		 * output (<n, i_{n}>, local count)
		 * @param key (mode, row index) <n, i_{n}>
		 * @param values 1
		 * @param context
		 */
		@Override
		public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			int count = 0;
			for(IntWritable value : values){
				count += value.get();
			}
			intWritable.set(count);
			context.write(key, intWritable);
		}
	}

	/**
	 * reducer of the first step of greedy row assignment: calculating (n, i_{n}) -> |\Omega^{(n)}_{i_{n}}|
	 */
	public static class GreedyReducer extends Reducer<PairWritable, IntWritable, Text, Text> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private Text keyText = new Text(""); 
		private Text valueText = new Text("");

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * aggregate the result of all machines.
		 * output (<n, i_{n}>, global count).
		 * @param key (mode, row index)  <n, i_{n}>
		 * @param values local count
		 * @param context
		 */
		@Override
		public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			int count = 0;
			for(IntWritable value : values){
				count += value.get();
			}

			keyText.set(key.left+","+key.right+","+count);

			context.write(keyText, valueText);
		}

	}

	/**
	 * mapper of the second step of greedy row assignment
	 * aggregate the results of the first step to one reducer
	 */
	public static class Greedy2Mapper extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private PairWritable keyWritable = new PairWritable(); // (mode, row index)
		private IntWritable intWritable = new IntWritable(); // number of observable entries in corresponding fiber

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * output the number of observable entries in each fiber (<n, i_{n}>, global count)
		 * @param key line number
		 * @param value line
		 * @param context
		 */
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().trim().split(",");
			keyWritable.set(Integer.valueOf(tokens[0]), Integer.valueOf(tokens[1]));
			intWritable.set(Integer.valueOf(tokens[2]));
			context.write(keyWritable, intWritable);

		}
	}

	/**
	 * reducer of the second step of greedy row assignment
	 * run greedy row assignment
	 */
	public static class Greedy2Reducer extends Reducer<PairWritable, IntWritable, NullWritable, NullWritable> {

		////////////////////////////////////
		// private fields
		////////////////////////////////////
		
		private int N; //dimension
		private int[] modeLengths; // n -> I_{n}
		private int[][] cardinalities; // (n, i_{n}) -> |\Omega^{(n)}_{i_{n}}|
		private int M; //number of reducers

		////////////////////////////////////
		// public methods
		////////////////////////////////////
		
		/**
		 * load parameters
		 */
		@Override
		public void setup(Context context){
			Configuration conf = context.getConfiguration();
			N = conf.getInt(P_N, 0);
			M = conf.getInt(P_M, 0);
			modeLengths = new int[N];
			cardinalities = new int[N][];
			for(int n=0; n<N; n++){
				modeLengths[n] = conf.getInt(P_MODELENGTH+(n+1), 0);
				cardinalities[n] = new int[modeLengths[n]];
			}
		}

		Text keyText = new Text("");
		Text valueText = new Text("");

		/**
		 * load the number of observable entries in each fiber into memory
		 * @param key <n, i_{n}>
		 * @param values global count
		 * @param context
		 */
		@Override
		public void reduce(PairWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

			for(IntWritable value : values){
				cardinalities[key.left][key.right] = value.get();
			}
		}

		/**
		 * main procedure of greedy row assignment.
		 * reorder the rows of factor matrices so that they are split across machines equally
		 * output (order of the factor matrix, row order) -> reordered row order 
		 * @param context
		 */
		@Override
		public void cleanup(Context context) throws IOException{

			Configuration conf = context.getConfiguration();
			
			String userHome = System.getProperty("user.home");
			String tempFile = userHome+"/PEGASUS_TEMP_"+context.getJobID()+"_"+System.currentTimeMillis();
			
			BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
			
			Random rand = new Random(0);

			int[] totalValueSumOfEachReducer = new int[M]; // m -> |{}_{m}\Omega|

			for(int n=N-1; n>=0; n--){
				context.progress();
				long modeLength = cardinalities[n].length; // I_{n}
				int[] sortedValue = cardinalities[n];
				ArrayIndexComparator comparator = new ArrayIndexComparator(sortedValue);
				
				/*
				 * sort rows in decreasing order of n
				 * i_{n} -> |\Omega^{(n)}_{i_{n}}|
				 */
				Integer[] sortedIndex = comparator.createIndexArray();
				Arrays.sort(sortedIndex, comparator);
				Arrays.sort(sortedValue);
				ArrayMethods.reverse(sortedValue);

				int[] currentValueSumOfEachReducer = new int[M]; // m -> |{}_{m}\Omega^{(n)}|
				int[] currentIndexOfEachReducer = new int[M]; // m -> |{}_{m}S_{n}|
				int[] maxIndexOfEachReducer = new int[M]; // m -> the maximum number of rows assigned to machine m
				int[][] indiciesOfEachReducer = new int[M][]; // (m, i) -> ith row of A^{(n)} assigned to machine m 
				for(int r=0; r<M; r++){
					maxIndexOfEachReducer[r] = (int)((long)Math.ceil((r+1)*modeLength/(0.0+M)) - (long)Math.ceil(r*modeLength/(0.0+M)));
					indiciesOfEachReducer[r] = new int[maxIndexOfEachReducer[r]];
				}

				for(int i=0; i<modeLength; i++){
					int smallestIndex = 0;
					int smallestValue = Integer.MAX_VALUE;
					int smallestTotalValue = Integer.MAX_VALUE;
					int smallestRemainingIndex = Integer.MAX_VALUE;
					LinkedList<Integer> tieList = new LinkedList<Integer>();
					for(int r=0; r<M; r++){

						int remainingIndex = maxIndexOfEachReducer[r]-currentIndexOfEachReducer[r];

						if(currentIndexOfEachReducer[r]<maxIndexOfEachReducer[r]){ // select the machines with |{}_{m}S_{n}| is smaller than maximum value
							if(currentValueSumOfEachReducer[r] < smallestValue){ // select the machine with the smallest |{}_{m}\Omega^{(n)}|
								smallestIndex = r;
								smallestValue = currentValueSumOfEachReducer[r];
								smallestTotalValue = totalValueSumOfEachReducer[r];
								smallestRemainingIndex = remainingIndex;
								tieList.clear();
								tieList.add(r);
							}
							else if(currentValueSumOfEachReducer[r] == smallestValue){ 
								if(remainingIndex < smallestRemainingIndex){ // select the machine with the smallest |{}_{m}S_{n}|
									smallestIndex = r;
									smallestValue = currentValueSumOfEachReducer[r];
									smallestTotalValue = totalValueSumOfEachReducer[r];
									smallestRemainingIndex = remainingIndex;
									tieList.clear();
									tieList.add(r);
								}
								else if(remainingIndex == smallestRemainingIndex){ 
									if(totalValueSumOfEachReducer[r] < smallestTotalValue){ // select the machine with the smallest |{}_{m}\Omega|
										smallestIndex = r;
										smallestValue = currentValueSumOfEachReducer[r];
										smallestTotalValue = totalValueSumOfEachReducer[r];
										smallestRemainingIndex = remainingIndex;
										tieList.clear();
										tieList.add(r);
									}
									else if(totalValueSumOfEachReducer[r] == smallestTotalValue){
										tieList.add(r);
									}
								}
							}
						}
					}

					smallestIndex = tieList.get(rand.nextInt(tieList.size()));

					currentValueSumOfEachReducer[smallestIndex] += sortedValue[i];
					totalValueSumOfEachReducer[smallestIndex] += sortedValue[i];
					indiciesOfEachReducer[smallestIndex][currentIndexOfEachReducer[smallestIndex]]=sortedIndex[i];
					currentIndexOfEachReducer[smallestIndex]++;

				}

				int index=0;
				for(int r=0; r<M; r++){
					for(int i=0; i<indiciesOfEachReducer[r].length; i++){
						bw.write(n+","+indiciesOfEachReducer[r][i]+","+index+"\n"); // reorder rows
						index++; 
					}
				}
			}
			
			bw.close();
			
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(true, new Path(tempFile), new Path(conf.get(P_INDEXMAP)));
			fs.close();
			
		}

		/**
		 * comparator class to sort indices according to their values
		 */
		static class ArrayIndexComparator implements Comparator<Integer>
		{
			private final int[] value;

			public ArrayIndexComparator(int[] value)
			{
				this.value = value;
			}

			public Integer[] createIndexArray()
			{
				Integer[] indexes = new Integer[value.length];
				for (int i = 0; i < value.length; i++)
				{
					indexes[i] = i;
				}
				return indexes;
			}

			public int compare(Integer index1, Integer index2)
			{
				return value[index2] - value[index1];
			}
		}	
	}
}
