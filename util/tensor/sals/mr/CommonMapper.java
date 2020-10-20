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

import static util.tensor.sals.mr.Params.P_DELIM;
import static util.tensor.sals.mr.Params.P_INDEX;
import static util.tensor.sals.mr.Params.P_INDEX_VALUE;
import static util.tensor.sals.mr.Params.P_M;
import static util.tensor.sals.mr.Params.P_MODELENGTH;
import static util.tensor.sals.mr.Params.P_N;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class of CDTF, SALS, and ALS Hadoop version. 
 * <P>
 * @author Kijung
 */
abstract public class CommonMapper extends Mapper<LongWritable, Text, TripleWritable, ElementWritable> {

	////////////////////////////////////
	// public fields
	////////////////////////////////////
	
	public final static int TYPE_TRAINING = 0;
	public final static int TYPE_TEST = 1;
	public final static int TYPE_QUERY = 2;
	
	////////////////////////////////////
	// private fields
	////////////////////////////////////
	
	private int M; // the number of machines
	
	private String delim; //delimeter
	private int[] modeIdx;	// column index of each mode input files
	private int valueIdx;	// column index of value in input files
	
	private int N; // dimension 
	private int[] modeLengths; // n -> I_{n}
	
	private TripleWritable position; // (machine index, mode, row index)
	private ElementWritable entry; // index of entries and value
	
	private int[][] permutedIdx = null; // result of greedy row assignment
	
	private int type; // mapper type: training / test / query
	
	////////////////////////////////////
	// public methods
	////////////////////////////////////
	
	/**
	 * load parameters and the result of greedy assignment
	 */
	@Override
	public void setup(Context context) throws IOException{
		
		type = getType();
		
		Configuration conf = context.getConfiguration();
		N = conf.getInt(P_N, 0);
		permutedIdx = new int[N][];
		
		ElementWritable.N = N;
		position = new TripleWritable();
		entry = new ElementWritable();
		
		modeIdx = new int[N];
		modeLengths = new int[N];
		
		delim = conf.get(P_DELIM," ");
		for(int i=0; i<N; i++){
			modeIdx[i] = conf.getInt(P_INDEX+(i+1), 0);
			modeLengths[i] = conf.getInt(P_MODELENGTH+(i+1), 0);
			permutedIdx[i] = new int[modeLengths[i]];
		}
		
		valueIdx = conf.getInt(P_INDEX_VALUE, 0);
		M = conf.getInt(P_M, 0);
		
		/*
		 * load the result of greedy row assignment
		 */
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
	}
	
	
	/**
	 * parse entry from file and reorder their index according to the result of greedy row assignment.
	 * output (<machine to assign this entry, order of the factor matrix that this entry is used to update>, <indices of the entry, value of the entry>).
	 * @param key	line number
	 * @param value	line
	 */
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(delim);
		int[] index = new int[N];
		int[] bIndex = new int[N];
		for(int i=0; i<N; i++){
			index[i] = permutedIdx[i][Integer.valueOf(tokens[modeIdx[i]])];
			bIndex[i] = Math.min(index[i]*M/modeLengths[i], M-1);
		}
		
		float val = Float.NaN; // write NaN as a value of each query entry 
		if(type!=TYPE_QUERY){
			val = Float.valueOf(tokens[valueIdx]);
		}
		
		boolean isTraining = type==TYPE_TRAINING;
		entry.set(index, val, isTraining);
		for(int i=0; i<N; i++){
			position.set(bIndex[i], i, index[i]);
			context.write(position, entry);
			if(!isTraining) // test and query entries are only distributed according to their first mode index 
				break;
		}
		
	}
	
	////////////////////////////////////
	// protected methods
	////////////////////////////////////
	
	/**
	 * get type of the mapper (the type of data that this mapper handles)
	 * @return	type of the mapper
	 */
	protected abstract int getType();
	
	/**
	 * Mapper for training data
	 *
	 */
	public static class TrainingMapper extends CommonMapper{
		@Override
		protected int getType() {
			return TYPE_TRAINING;
		}
	}
	
	/**
	 * Mapper for test data
	 *
	 */
	public static class TestMapper extends CommonMapper{
		@Override
		protected int getType() {
			return TYPE_TEST;
		}
	}
	
	/**
	 * Mapper for query data
	 *
	 */
	public static class QueryMapper extends CommonMapper{
		@Override
		protected int getType() {
			return TYPE_QUERY;
		}
	}
}


