package util.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosineSimilarity {
	private static final Logger logger = LoggerFactory.getLogger( CosineSimilarity.class);

	public static double compute(float[] from, float[] to) {
		if(from.length!=to.length) {
			logger.error("dimension different");
			
			return -1;
		}
		
		/**
		 * vector
		 */
		 double innerProduct = 0.0, thisPower2 = 0.0, thatPower2 = 0.0;
	        for (int i = 0; i < from.length; i++) {
	            innerProduct += from[i] * to[i];
	            thisPower2 += Math.pow(from[i], 2) ;
	            thatPower2 += Math.pow(to[i],2);// * thatList.get(i).doubleValue();
	        }
	        return innerProduct / Math.sqrt(thisPower2 * thatPower2);
	}
	/**
	 * avg of cosine similarity of each pair of factor vectors.
	 * factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * N dimension
	 * K rank
	 * @return
	 */
	public static double compute(float[][][] paramsEvent1,float[][][] paramsEvent2, 
			int actor1IndexEvent1,int actor2IndexEvent1,int action1IndexEvent1,int dateIndexEvent1,
			int actor1IndexEvent2,int actor2IndexEvent2,int action1IndexEvent2,int dateIndexEvent2,int N,int K) {
		
		float[] actor1E1 = paramsEvent1[0][actor1IndexEvent1];
		float[] actor2E1 = paramsEvent1[1][actor2IndexEvent1];
		float[] actionE1 = paramsEvent1[2][action1IndexEvent1];
		float[] dateE1 =  paramsEvent1[3][dateIndexEvent1];
		
		float[] actor1E2 = paramsEvent2[0][actor1IndexEvent2];
		float[] actor2E2 = paramsEvent2[1][actor2IndexEvent2];
		float[] actionE2 = paramsEvent2[2][action1IndexEvent2];
		float[] dateE2 =  paramsEvent2[3][dateIndexEvent2];
		
		double sim1 = compute(actor1E1,actor1E2);
		double sim2 = compute(actor2E1, actor2E2);
		double sim3 = compute(actionE1,actionE2);
		double sim4 = compute(dateE1,dateE2);
		return (sim1+sim2+sim3+sim4)/4;
	}
	
	/**
	 * avg of cosine similarity of each pair of factor vectors.
	 * factor matrices (n, i_{n}, k) -> a^{(n)}_{i_{n}k}
	 * N dimension
	 * K rank
	 * @return
	 */
	public static double compute(float[][][] paramsEvent,
			int actor1IndexEvent1,int actor2IndexEvent1,int action1IndexEvent1,int dateIndexEvent1,
			int actor1IndexEvent2,int actor2IndexEvent2,int action1IndexEvent2,int dateIndexEvent2,int N,int K) {
		
		float[] actor1E1 = paramsEvent[0][actor1IndexEvent1];
		float[] actor2E1 = paramsEvent[1][actor2IndexEvent1];
		float[] actionE1 = paramsEvent[2][action1IndexEvent1];
		float[] dateE1 =  paramsEvent[3][dateIndexEvent1];
		
		float[] actor1E2 = paramsEvent[0][actor1IndexEvent2];
		float[] actor2E2 = paramsEvent[1][actor2IndexEvent2];
		float[] actionE2 = paramsEvent[2][action1IndexEvent2];
		float[] dateE2 =  paramsEvent[3][dateIndexEvent2];
		
		double sim1 = compute(actor1E1,actor1E2);
		double sim2 = compute(actor2E1, actor2E2);
		double sim3 = compute(actionE1,actionE2);
		double sim4 = compute(dateE1,dateE2);
		return (sim1+sim2+sim3+sim4)/4;
	}
	
	/**
	 * cosine similarity
	 * @param paramsEvent
	 * @param index1
	 * @param index2
	 * @return
	 */
	public static double compute(float[][][] paramsEvent,int[] index1, int[] index2) {
		
		int actor1IndexEvent1 = index1[0];
		int actor2IndexEvent1 = index1[1];
		int action1IndexEvent1 = index1[2];
		int dateIndexEvent1 = index1[3];
		
		int actor1IndexEvent2 = index2[0];
		int actor2IndexEvent2 = index2[1];
		int action1IndexEvent2 = index2[2];
		int dateIndexEvent2 = index2[3];
		
		float[] actor1E1 = paramsEvent[0][actor1IndexEvent1];
		float[] actor2E1 = paramsEvent[1][actor2IndexEvent1];
		float[] actionE1 = paramsEvent[2][action1IndexEvent1];
		float[] dateE1 =  paramsEvent[3][dateIndexEvent1];
		
		float[] actor1E2 = paramsEvent[0][actor1IndexEvent2];
		float[] actor2E2 = paramsEvent[1][actor2IndexEvent2];
		float[] actionE2 = paramsEvent[2][action1IndexEvent2];
		float[] dateE2 =  paramsEvent[3][dateIndexEvent2];
		
		double sim1 = compute(actor1E1,actor1E2);
		double sim2 = compute(actor2E1, actor2E2);
		double sim3 = compute(actionE1,actionE2);
		double sim4 = compute(dateE1,dateE2);
		return (sim1+sim2+sim3+sim4)/4;
	}
	
	/**
	 * by dim
	 * @param paramsEvent
	 * @param index1
	 * @param index2
	 * @return
	 */
	public static double[] computeByDim(float[][][] paramsEvent,int[] index1, int[] index2) {
		
		int actor1IndexEvent1 = index1[0];
		int actor2IndexEvent1 = index1[1];
		int action1IndexEvent1 = index1[2];
		int dateIndexEvent1 = index1[3];
		
		int actor1IndexEvent2 = index2[0];
		int actor2IndexEvent2 = index2[1];
		int action1IndexEvent2 = index2[2];
		int dateIndexEvent2 = index2[3];
		
		float[] actor1E1 = paramsEvent[0][actor1IndexEvent1];
		float[] actor2E1 = paramsEvent[1][actor2IndexEvent1];
		float[] actionE1 = paramsEvent[2][action1IndexEvent1];
		float[] dateE1 =  paramsEvent[3][dateIndexEvent1];
		
		float[] actor1E2 = paramsEvent[0][actor1IndexEvent2];
		float[] actor2E2 = paramsEvent[1][actor2IndexEvent2];
		float[] actionE2 = paramsEvent[2][action1IndexEvent2];
		float[] dateE2 =  paramsEvent[3][dateIndexEvent2];
		
		double sim1 = compute(actor1E1,actor1E2);
		double sim2 = compute(actor2E1, actor2E2);
		double sim3 = compute(actionE1,actionE2);
		double sim4 = compute(dateE1,dateE2);
		
		return  new double[] { (sim1+sim2+sim3+sim4)/4};
	}
	
}
