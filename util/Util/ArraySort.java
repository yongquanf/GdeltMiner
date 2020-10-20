package util.Util;

import java.util.Arrays;
//<T extends Comparable>

public class ArraySort {

	class Pair implements Comparable<Pair>{

		public final int index;
		public final int value;
		
		public Pair(int _index,int _value) {
			this.index = _index;
			this.value = _value;
		}
		
		/**
		 * descending order
		 */
		public int compareTo(Pair other) {
			// TODO Auto-generated method stub
			return -1*(Integer.valueOf(this.value).compareTo(other.value));
		}
		
		
	}//
	/**
	 * descending sort
	 * @param val
	 * @return
	 */
	public int[] descendingSortReturnIndex(int[] val) {
		int len = val.length;
		Pair[] dat = new Pair[len];
		
		for(int i=0;i<val.length;i++) {
			dat[i] = new Pair(i,val[i]);
		}
		Arrays.parallelSort(dat);
		int[] ind = new int[dat.length];
		for(int i=0;i<dat.length;i++) {
			ind[i] = dat[i].index;
		}
		dat = null;
		return ind;
	}
}
