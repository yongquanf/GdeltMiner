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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * Common interface of multi-threaded jobs
 * <P>
 * @author Kijung
 */
public abstract class MultiThread<T> {

	////////////////////////////////////
	//public methods
	////////////////////////////////////
	
	/**
	 * process the given job
	 * this method should be overwritten
	 * @param jobIndex index of a job to process
	 * @param threadIndex	index of a thread processing this job
	 * @return	output of the job
	 */
	public abstract T runJob(int jobIndex, int threadIndex);
	
	/**
	 * execute jobs in jobList using the given number of threads
	 * @param threadNum	number of threads to execute jobs
	 * @param jobList	list of jobs to execute
	 * @return	job id -> output of the job
	 */
	public HashMap<Integer, T> run(int threadNum, List<Integer> jobList){	
		
		m_finishingJobs = 0;
		m_exception = null;
		m_jobList = jobList;
		m_numberOfJobs = jobList.size();
		m_threads = new Thread[threadNum];
		m_results = new HashMap<Integer, T>();
		m_lock = new Object();
		
		for(int i=0; i<threadNum; i++){
				
			final int threadIndex = i;
			
			Thread t = new Thread(){
				@Override
				public void run(){
					while(true){
						int jobIndex = 0;
						jobIndex = getJob();
						if(jobIndex<0){
							break;
						}
						try{
							T result = runJob(jobIndex, threadIndex);
							synchronized(m_lock){
								m_finishingJobs++;
								m_lock.notifyAll();
							}
							
							if(result!=null)
								setResult(jobIndex, result);
						}
						catch(Exception e){
							synchronized(m_lock){
								m_exception = e;
								m_lock.notifyAll();
							}
							return;
						}
					}
				}
			};
			t.start();
			m_threads[i] = t;
		}
		
		int finishingJobs = 0;
		while(true){
			
			if(m_finishingJobs > finishingJobs){
				finishingJobs = m_finishingJobs;
			}
			else{
				synchronized(m_lock){
					if(m_exception!=null || m_finishingJobs==m_numberOfJobs){
						break;
					}
					else{
						try {
							m_lock.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		
		if(m_exception!=null){
			throw new RuntimeException(m_exception);
		}
		
		return m_results;
	}
	
	/**
	 * create the list of n jobs
	 * @param n
	 * @return jobList
	 */
	public static List<Integer> createJobList(int n){
		LinkedList<Integer> jobList = new LinkedList<Integer>();
		for(int i=0; i<n; i++){
			jobList.add(i);
		}
		return jobList;
	}
	
	////////////////////////////////////
	//private fields
	////////////////////////////////////
	
	private Thread[] m_threads; // list of threads
	
	private HashMap<Integer, T> m_results; // job id -> result
	
	private int m_numberOfJobs;	// number of total jobs
	
	private int m_finishingJobs; // number of finished jobs
	
	private Exception m_exception; // exception
	
	private List<Integer> m_jobList; // list of unprocessed jobs
	
	private Object m_lock; // lock object
	
	////////////////////////////////////
	//private methods
	////////////////////////////////////
	
	/**
	 * get an unprocessed job
	 * @return return -1 if there is no unprocessed job left, return job index otherwise
	 */
	private int getJob(){
		
		synchronized(m_lock){
			
			if(m_exception!=null)
				return -1;
			else if(m_jobList.isEmpty()){
				return -1;
			}
			else{
				return m_jobList.remove(0);
			}
		}
	}
	
	/**
	 * add the result of the given job
	 * @param jobIndex
	 * @param result
	 */
	private void setResult(int jobIndex, T result){
		synchronized(m_results){
			m_results.put(jobIndex, result);
		}
	}	
	
}
