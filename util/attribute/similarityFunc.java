package util.attribute;

import java.util.Date;
import java.util.List;

import util.geo.Location;
import util.models.GdeltEventResource;

public abstract class similarityFunc{

	/**
	 * normalize, 1, sigmoid
	 */
	public static int NormalizeChoice = 1;
	/**
	 * similarity
	 * @param e1
	 * @param e2
	 * @return
	 */
	public List<Double> simCalc(GdeltEventResource e1,GdeltEventResource e2){
		
		return null;
	}
	/**
	 * actor1, actor2
	 * @param s1
	 * @param s2
	 * @return
	 */
	public double EventActorDyadicDiff(String s1,String s2){
		return -1;
	}
	
	/**
	 * diffby seconds
	 * @param d1
	 * @param d2
	 * @return
	 */
	public double EventDateDiff(Date d1,Date d2){
		return   normalize((Math.abs(d1.getTime()-d2.getTime())/1000.0));
	}
	
	/**
	 * geodistance
	 * @param actionGeo1
	 * @param actionGeo2
	 * @return
	 */
	public double EventActionGeoDiff(double actionGeo1Lati,double actionGeo1Long,double actionGeo2Lati,double actionGeo2Long ){
		Location loc1 = new Location(actionGeo1Lati,actionGeo1Long);
		Location loc2 = new Location(actionGeo2Lati,actionGeo2Long);
		return   normalize(Math.abs(loc1.distance(loc2).getValue()));
	}
	
	
	
	/**
	 * num metric, should be >=0
	 * @param avgTone, numArticles, numSources, numMention, goldsteinScale, quadClass
	 * @param  
	 * @return
	 */
	public double EventNumDiff(double metric1,double metric2){
		//
		if(Double.isNaN(metric1)||Double.isNaN(metric2)){
			return -1;
		}
		
		return   normalize(Math.abs(metric1-metric2));
	}
	
	/**
	 * 
	 * @return
	 */
	public static double normalize(double x){
		if(NormalizeChoice == 1){
			return sigmoid(x);
		}else{
			return x;
		}
	}
	/**
	 * regularize
	 * @param x
	 * @return
	 */
	public static double sigmoid(double x){
		return 1.0/(1.0+Math.exp(-x));
	}
}
