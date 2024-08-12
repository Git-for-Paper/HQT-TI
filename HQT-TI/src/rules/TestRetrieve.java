package rules;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.util.concurrent.ConcurrentNavigableMap;

public class TestRetrieve {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DB db;
		db = DBMaker.newFileDB(new File("rules.Test")).cacheDisable()
	               .closeOnJvmShutdown().
	               make();
		
		long start = System.currentTimeMillis();
		
		ConcurrentNavigableMap map = db.getTreeMap("1");
		int key = (int) map.firstKey();
		int[] buf = (int[]) map.get(key);
		for(int i=0; i<buf.length; i++){
		//	System.out.println(buf[i]);
		}
		while(map.higherKey(key)!=null){
			key = (int) map.higherKey(key);
		    buf = (int[]) map.get(key);
			for(int i=0; i<buf.length; i++){
		//		System.out.println(buf[i]);
			}
		}
		long end = System.currentTimeMillis();
		System.err.println("Seconds: " + ((end - start) / 1000.0f));

	}

}
