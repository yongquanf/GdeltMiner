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
 * Methods for handling arrays
 * <P>
 * @author Kijung
 */
public class ArrayMethods {
	
	////////////////////////////////////
	//public methods
	////////////////////////////////////
	
	/**
	 * deep copy
	 * @param input	array to copy
	 * @return	copied array
	 */
	public static int[][] copy(int[][] input){
		int[][] result = new int[input.length][];
		for(int i=0; i<input.length; i++){
			result[i] = input[i].clone();
		}
		return result;
	}
	
	/**
	 * deep copy
	 * @param input	array to copy
	 * @return	copied array
	 */
	public static float[][] copy(float[][] input){
		float[][] result = new float[input.length][];
		for(int i=0; i<input.length; i++){
			result[i] = input[i].clone();
		}
		return result;
	}
	
	/**
	 * deep copy
	 * @param input	array to copy
	 * @return	copied array
	 */
	public static double[][] copy(double[][] input){
		double[][] result = new double[input.length][];
		for(int i=0; i<input.length; i++){
			result[i] = input[i].clone();
		}
		return result;
	}
	
	/**
	 * deep copy
	 * @param input	array to copy
	 * @return	copied array
	 */
	public static float[][][] copy(float[][][] input){
		float[][][] result = new float[input.length][][];
		for(int i=0; i<input.length; i++){
			result[i] = copy(input[i]);
		}
		return result;
	}
	
	/**
	 * create random matrix
	 * @param m	row number of created matrix
	 * @param n	column number of created matrix
	 * @param scalarFactor Scalar factor multiplied to each element
	 * @return	created matrix
	 */
	public static float[][] createUniformRandomMatrix(int m, int n, float scalarFactor, Random random){
		float[][] matrix = new float[m][n];
		for(int i=0; i<m; i++){
			for(int j=0; j<n; j++){
				matrix[i][j] = (random.nextFloat())*scalarFactor;
				if(matrix[i][j]==0){
					matrix[i][j] = 0.00001f*scalarFactor;
				}
			}
		}
		return matrix;
	}
	
	/**
	 * create random vector
	 * @param n	number of entries
	 * @param scalarFactor Scalar factor multiplied to each element
	 * @return	created vector
	 */
	public static float[] createUniformRandomVector(int n, float scalarFactor, Random random){
		float[] vector = new float[n];
		for(int i=0; i<n; i++){
			vector[i] = (random.nextFloat()+0.00001f)*scalarFactor;
		}		
		return vector;
	}
	
	
	 /**
     * Reverses the order of the given array.
     * @param array  the array to reverse
     */
    public static void reverse(int[] array) {
        if (array == null) {
            return;
        }
        int i = 0;
        int j = array.length - 1;
        int tmp;
        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }
    
    /**
	 * create a length N sequential list
	 * @param N	length of the list
	 * @return	created sequence
	 */
	public static int[] createSequnce(int N){
		int[] result = new int[N];
		for(int i=0; i<N; i++){
			result[i] = i;
		}
		return result;
	}
	
}
