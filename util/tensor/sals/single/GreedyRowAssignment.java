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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Random;

/**
 * Greedy row assignment of CDTF single machine version
 * <P>
 * @author Kijung
 */
public class GreedyRowAssignment {

	////////////////////////////////////
	//public methods
	////////////////////////////////////
	
	/**
	 * greedy row assignment
	 * reorder the rows of factor matrices so that they are split across machines equally
	 * 
	 * @param inputPath	input file path
	 * @param N	dimension
	 * @param modesIdx	column index of ith mode index
	 * @param modeLengths	n -> I_{n}
	 * @param M	number of cores
	 * @return	(order of the factor matrix, row order) -> reordered row order 
	 */
	public static int[][] run(String inputPath, int N, int[] modesIdx, int[] modeLengths, int M){
		
		Random rand = new Random(0);
		
		int[][] cardinalities = new int[N][];
		int[][] permutedIdx = new int[N][];
		for(int dim=0; dim<N; dim++){
			System.out.println("dim: "+dim+", len: "+modeLengths[dim]);
			cardinalities[dim] = new int[modeLengths[dim]];
			permutedIdx[dim] = new int[modeLengths[dim]];
		}
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(inputPath));

			while(true){
				String line = br.readLine();
				if(line==null)
					break;
				String[] tokens = line.split(",");
				for(int dim=0; dim<N; dim++){
					System.out.println("dim: "+dim+", modesIdx[dim]"+modesIdx[dim]+", tokensLen: "+tokens.length+", tokens[dim]: "+tokens[dim]);
					cardinalities[modesIdx[dim]][Integer.valueOf(tokens[dim])]++;
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try { br.close(); } catch (IOException e) { e.printStackTrace(); }
		}
		
		
		int[] totalValueSumOfEachReducer = new int[M]; // m -> |{}_{m}\Omega|
		
		for(int n=N-1; n>=0; n--){
			
			long modeLength = cardinalities[n].length; // I_{n}
			
			/*
			 * sort rows in decreasing order of n
			 * i_{n} -> |\Omega^{(n)}_{i_{n}}|
			 */
			int[] sortedValue = cardinalities[n];
			ArrayIndexComparator comparator = new ArrayIndexComparator(sortedValue);
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
				//find a reducer who has smallest valueSum 
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
					permutedIdx[n][indiciesOfEachReducer[r][i]] = index++; // reordered row index
					
				}
			}
		}
		
		return permutedIdx;
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
