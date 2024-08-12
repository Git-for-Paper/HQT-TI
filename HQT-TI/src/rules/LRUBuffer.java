package rules;

import java.util.HashMap;
import java.util.TreeMap;


public class LRUBuffer {
	
	public static int MaxBlock = 262144;
	private static TreeMap<Integer, Long> lru = new TreeMap<Integer, Long>();
	private static HashMap<Long, Integer> pool = new HashMap<Long, Integer>();
	
	public static void Init(){
		lru.put(0, 0l);
		pool.put(0l, 0);
	}
	
	public static boolean IsHit(String term, int pid){
		if(MaxBlock == 0){
			return false;
		}
		
		int tid = Integer.valueOf(term);
		long cid = tid*Integer.MAX_VALUE + pid;
		if(pool.containsKey(cid)){
			int prerank = pool.get(cid);
			int currank = lru.lastKey()+1;
			lru.put(currank, cid);
			lru.remove(prerank);
			pool.put(cid, currank);
			return true;
		}
		else{
			if(lru.size() >= MaxBlock){
				int firstkey = lru.firstKey();
				long leastuseid = lru.get(firstkey);
				pool.remove(leastuseid);
				lru.remove(firstkey);
				int currank = lru.lastKey()+1;
				lru.put(currank, cid);
				pool.put(cid, currank);
			}
			else{
				int currank = lru.lastKey()+1;
				lru.put(currank, cid);
				pool.put(cid, currank);
			}
			return false;
		}
	}
}
