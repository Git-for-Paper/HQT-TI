package rules;

import me.lemire.integercompression.*;
import mvs.rule1;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

public class BuildSFCQ {

	/**
	 * @param args
	 */
	public static Vector<Integer> tnum;
	public static int TotalKeywords = 41270;
	public static int difKeuword = 41270;
	public static int rulenum = 5000;
	public static int BlockSize = 64;
	public static IntegratedIntegerCODEC codec =  new 
	           IntegratedComposition(
	                    new IntegratedBinaryPacking(),
	                    new IntegratedVariableByte());
	public static int[] Compress(int[] pagefile){
		int[] data = pagefile;
        int [] compressed = new int[data.length];
        IntWrapper inputoffset = new IntWrapper(0);
        IntWrapper outputoffset = new IntWrapper(0);
        codec.compress(data,inputoffset,data.length,compressed,outputoffset);
        System.out.println("compressed from "+data.length*4/1024f+"KB to "+outputoffset.intValue()*4/1024f+"KB");
        // we can repack the data: (optional)
        compressed = Arrays.copyOf(compressed,outputoffset.intValue());
        return compressed;
	}
	public static int[] uncompress(int[] pagefile){
		int[] recovered = new int[BuildSFCQ.BlockSize];
        IntWrapper recoffset = new IntWrapper(0);
        BuildSFCQ.codec.uncompress(pagefile,new IntWrapper(0),pagefile.length,recovered,recoffset);
		return recovered;
	}
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
	//	String location_file = args[0];
		String text_file = "****";
		rule1.generate_rule();
		BufferedReader brt = new BufferedReader(new InputStreamReader(new FileInputStream(text_file)));
		TotalKeywords = TotalKeywords+rulenum;
		System.out.println("   "+TotalKeywords);
		int[] count = new int[TotalKeywords];
		for(int i=0; i<count.length; i++){
			count[i] = BlockSize + 1;
		}
		
		int id = 0;
		double x, y;
	
		long start = System.currentTimeMillis();
		String line;
		String[] temp;
		
		DB db;
		db = DBMaker.newFileDB(new File("SFCQ"))
	               .closeOnJvmShutdown().
	               make();
		System.out.println(TotalKeywords);
		for(int i=0; i<TotalKeywords; i++){
			ConcurrentNavigableMap<Integer,int[]> map = db.getTreeMap(String.valueOf(i));
		}
		String str;
		while ((str = brt.readLine()) != null){
			tnum = new Vector<>();
			temp = str.split(",");
			Set<Integer> s = new HashSet<>();
			for(int k = 1; k < temp.length; k++){
				String word = temp[k];
				ConcurrentNavigableMap<Integer,int[]> map = db.getTreeMap(word);
				int word_int = Integer.parseInt(word);
				s.add(word_int);
				if(count[word_int] >= BlockSize){
					count[word_int] = 0;
					int[] pagefile = new int[BlockSize];
					pagefile[count[word_int]] = id;
					count[word_int]++;
					map.put(id, pagefile);
				}
				else{
					int maxkey = map.lastKey();
					int[] pagefile = map.get(maxkey);
					pagefile[count[word_int]] = id;
					count[word_int]++;
					map.put(maxkey, pagefile);
				}
			}
			for(Map.Entry<Set,Integer> entry : rule1.mp.entrySet()){
				if(s.containsAll(entry.getKey())){
					tnum.add(entry.getValue());
				}
			}
			for(int i=0;i<tnum.size();i++){//
				int word_int = tnum.get(i)+difKeuword;
				ConcurrentNavigableMap<Integer,int[]> map = db.getTreeMap(Integer.toString(word_int));
				if(count[word_int] >= BlockSize){
					count[word_int] = 0;
					int[] pagefile = new int[BlockSize];
					pagefile[count[word_int]] = id;
					count[word_int]++;
					map.put(id, pagefile);

				}
				else{
					int maxkey = map.lastKey();
					int[] pagefile = map.get(maxkey);
					pagefile[count[word_int]] = id;
					count[word_int]++;
					map.put(maxkey, pagefile);
				}
			}
			id++;
		}
		long end = System.currentTimeMillis();
		db.commit();
		System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);
		System.err.println("Begin Compress...");
		for(int i=0; i<TotalKeywords; i++){
			ConcurrentNavigableMap<Integer,int[]> map = db.getTreeMap(String.valueOf(i));
			Set<Integer> S = map.keySet();
			Object[] KeySet = S.toArray();
			for(int j=0; j<KeySet.length; j++){
				if(j == KeySet.length - 1){
					int[] crash = map.get(KeySet[j]);
					for(int k=1; k<crash.length; k++){
						if(crash[k] == 0){
							for(int m = k; m<crash.length; m++){
								crash[m] = crash[k-1];
							}
							break;
						}
					}
					int[] compressed = Compress(crash);
					map.put((Integer) KeySet[j], compressed);
				}
				else{
					int[] compressed = Compress(map.get(KeySet[j]));
					map.put((Integer) KeySet[j], compressed);
				}
			}
			
		}
		db.commit();
		long end2 = System.currentTimeMillis();
		System.err.println("Minutes: " + ((end2 - start) / 1000.0f) / 60.0f);
	}


}
