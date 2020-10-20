package util.Cluster;

import java.util.Arrays;

import org.apache.commons.math3.ml.clustering.Clusterable;

/**
 * wrapper of the data
 * @author quanyongf
 *
 */
public class MultDimData implements Clusterable{
	 private double[] point;

	public  MultDimData(double[] _point){
		this.point = new double[_point.length];
		//deep copy
		System.arraycopy(_point, 0, point, 0,_point.length);
	}
	
	@Override
	public double[] getPoint() {
		// TODO Auto-generated method stub
		return this.point;
	}

	@Override
	public String toString() {
		return "[" + Arrays.toString(point) + "]";
	}
	
	
	
}
