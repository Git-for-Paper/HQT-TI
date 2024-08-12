package rules;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentNavigableMap;

public class Test {

	/**
	 * @param args
	 */
	
	public static int TotalKeywords = 68053;
	public static int BlockSize = 1024;
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DB db;
		db = DBMaker.newFileDB(new File("rules.Test"))
	               .closeOnJvmShutdown().
	               make();
		
		BufferedReader brt = new BufferedReader(new InputStreamReader(new FileInputStream(text_file)));
		
		int count = BlockSize + 1;
		int id = 0;
		double x, y;
	
		long start = System.currentTimeMillis();
		String line;
		String[] temp;
		ConcurrentNavigableMap<Integer,int[]> map = db.getTreeMap("1");
		String str;

		for(id = 1000000; id < 2000000; id++){
				if(count >= BlockSize){
					count = 0;
					int[] pagefile = new int[BlockSize];
					pagefile[count] = id;
					count++;
					map.put(id, pagefile);
				//	System.err.println(count);
				}
				else{
					int maxkey = map.lastKey();
					int[] pagefile = map.get(maxkey);
					pagefile[count] = id;
					count++;
					map.put(maxkey, pagefile);
				}
			
		//	id++;
		}
			
		long end = System.currentTimeMillis();
		db.commit();
		System.err.println("Seconds: " + ((end - start) / 1000.0f));

	}

}
