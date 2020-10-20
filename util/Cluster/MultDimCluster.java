package util.Cluster;

import java.util.List;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.RandomGenerator;

import com.google.common.collect.Lists;

import util.Util.smile.math.matrix.DenseMatrix;
 

public class MultDimCluster {

//	 public class oneDimData implements Clusterable{
//		 private double[] point;
// 
//		public  oneDimData(double _point){
//			this.point = new double[] {_point};
//		}
//		
//		@Override
//		public double[] getPoint() {
//			// TODO Auto-generated method stub
//			return point;
//		}
//	 }
	 
	public MultDimCluster(){
		
	}
	
	/**
	 * each row vector transformed to a point
	 * @param relCoordMatrix
	 * @return
	 */
	public static List<MultDimData> extractRelCoordinate(DenseMatrix relCoordMatrix){
		int nrows = relCoordMatrix.nrows();
		int ncols = relCoordMatrix.ncols();
		//array list
		List<MultDimData> out = Lists.newArrayList();
		for(int i=0;i<nrows;i++) {
			double[] point = relCoordMatrix.getRowVector(i,ncols);
			out.add( new MultDimData(point));
		}
		return out;
	}
	/**
	 * cluster one-dimensional data
	 * @param clusterInput
	 * @param kmeansClusterNum
	 * @param maxIters
	 * @return
	 */
	public static List<CentroidCluster<MultDimData>> clusterKMeansPP(List<MultDimData> clusterInput,
			int kmeansClusterNum,int maxIters){
		 
		
		EuclideanDistance distance = new EuclideanDistance();
		
		KMeansPlusPlusClusterer<MultDimData> clusterer = new 
				 KMeansPlusPlusClusterer<MultDimData>(kmeansClusterNum,maxIters,distance); 
		 
		 
		 
		    //seed
		    RandomGenerator rg = clusterer.getRandomGenerator();
		    rg.setSeed(System.currentTimeMillis());
		    
	
		    //compute cluster
		    List<CentroidCluster<MultDimData>> clusterResults = clusterer.cluster(
		    		clusterInput);
	return clusterResults;
	}
	
	
}
