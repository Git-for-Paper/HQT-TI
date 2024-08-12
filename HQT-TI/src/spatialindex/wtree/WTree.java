package spatialindex.wtree;

import spatialindex.spatialindex.ISpatialIndex;
import spatialindex.spatialindex.Region;
import spatialindex.storagemanager.*;
import storage.DocumentStore;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class WTree {

	public static Vector leaf;
	public static Hashtable objstore;
	public static int level;
	public static int minf;
	public static int maxf;
	public static int maxword = 5;

	static PropertySet ps, ps2;
	static IStorageManager diskfile;
	static IBuffer file;
	static RTree wtree;

	public static void main(String[] args) throws Exception
	{
//		if(args.length != 5){
//			System.out.println("Usage: WTree text_file location_file index_file page_size fanout");
//			System.exit(-1);
//		}
		String text_file = "E:\\program\\spatial_keyword_query\\src\\retail.dat";
		String location_file = "E:\\program\\spatial_keyword_query\\src\\spatial_PGPSW.txt";
		String index_file = "E:\\program\\wtree10\\w10";
		int page_size = 32000;
		int fanout = 4096;
		
		maxf = fanout;
		minf = maxf / 2;
		
		createStorage(index_file, page_size, fanout);
		level = 0;
		long start = System.currentTimeMillis();
		do{
			System.out.println("level:" + level);
			if(level == 0){
				leafnode(location_file, text_file);
			}
			else
				leafnode();
			
			rtreebignode();
			summarizeleafnode();
			
			if(leaf.size() == 1){
				writeRoot();
				break;
			}
			writeNode();
			clear();
			level++;
		}while(true);
		long end = System.currentTimeMillis();
		System.err.println("Minutes: " + ((end - start) / 1000.0f) / 60.0f);
		System.err.println("Index ID: " + wtree.m_headerID);	
		wtree.flush();
		
	}
	public static void createStorage(String index_file, int page_size, int fanout){
		try{
			PropertySet ps = new PropertySet();

			Boolean b = new Boolean(true);
			ps.setProperty("Overwrite", b);
			
			ps.setProperty("FileName", index_file + ".rtree");
			
			Integer i = new Integer(page_size);
			ps.setProperty("PageSize", i);
			
			diskfile = new DiskStorageManager(ps);

			file = new RandomEvictionsBuffer(diskfile, 10, false);
			
			PropertySet ps2 = new PropertySet();

			Double f = new Double(0.7);
			ps2.setProperty("FillFactor", f);

			i = new Integer(fanout);
			ps2.setProperty("IndexCapacity", i);
			ps2.setProperty("LeafCapacity", i);
				
			i = new Integer(2);
			ps2.setProperty("Dimension", i);

			wtree = new RTree(ps2, file);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void leafnode(String location_file, String text_file) throws Exception
	{
		FileInputStream fis = new FileInputStream(location_file);
		BufferedReader location_reader = new BufferedReader(new InputStreamReader(fis));
		
		DocumentStore ds = new DocumentStore(text_file);
		
		leaf = new Vector();
		objstore = new Hashtable();
		
		String line;
		String[] temp;
		double[] f1 = new double[2];
		double[] f2 = new double[2];
		double x, y;
		int id;
		
		WNode root = new WNode(0);
		
		while((line = location_reader.readLine()) != null){
			temp = line.split(",");
			id = Integer.parseInt(temp[0]);
			x = Double.parseDouble(temp[1]);
			y = Double.parseDouble(temp[2]);
			
			Vector document = ds.read(id);
			if(document == null){
				System.out.println("Couldn't find document " + id);
				System.exit(-1);
			}
			
			WNode n = new WNode(-1);
			n.id = id;
			for(int i = 0; i < document.size(); i++){
				//  mod by Lisi
				if((Integer)document.get(i) <= maxword){
					n.keyword.add((Integer)document.get(i));
				}
			}
				
			
			f1[0] = x; f1[1] = y;
			f2[0] = x; f2[1] = y;
			n.mbr = new Region(f1, f2);
			
			root.obj.add(n.id);
			objstore.put(n.id, n);
		}
		Vector v = partition(root);
		if(v.size() > 0){
			WNode n = new WNode(-1);
			n.obj.addAll(v);
			leaf.add(n);
		}
	}
	static void leafnode(){
		WNode root = new WNode(0);
		Iterator iter = objstore.keySet().iterator();
		while(iter.hasNext()){
			int id = (Integer)iter.next();
			root.obj.add(id);
		}
		
		Vector v = partition(root);
		if(v.size() > 0){
			WNode n = new WNode(-1);
			n.obj.addAll(v);
			leaf.add(n);
		}
		
	}
	
	public static Vector partition(WNode node){
		if(node.obj.size() <= maxf && node.obj.size() >= minf){
			leaf.add(node);
			Vector empty = new Vector();
			return empty;
		}
		else if(node.obj.size() < minf){
			return node.obj;
		}
		else{
			WNode n1;
			WNode n2;
			int level = node.wordlevel+1;
			
			do{
				n1 = new WNode(level);
				n2 = new WNode(level);
				
				for(int i = 0; i < node.obj.size(); i++){
					int oid = (Integer)node.obj.get(i);
					WNode kn = (WNode)objstore.get(oid);
					if(kn.keyword.contains(level-1)){
						n1.obj.add(oid);
					}
					else{
						n2.obj.add(oid);
					}
				}
				
				level++;
			}while((n1.obj.size() == 0 || n2.obj.size() == 0) && level < maxword);
			
			if(level == maxword){
				leaf.add(node);
				Vector empty = new Vector();
				return empty;
			}
			
			//System.out.println("n1:" + n1.obj.size());
			//System.out.println("n2:" + n2.obj.size());
			Vector v1 = new Vector();
			Vector v2 = new Vector();
			if(n1.obj.size() > 0)
				v1 = partition(n1);
			if(n2.obj.size() > 0)
				v2 = partition(n2);
			
			int t = v1.size() + v2.size();
			if(t <= maxf && t >= minf){
				WNode n = new WNode(-1);
				n.obj.addAll(v1);
				n.obj.addAll(v2);
				leaf.add(n);
				Vector empty = new Vector();
				return empty;
			}
			else if(t < minf){
				v1.addAll(v2);
				return v1;
			}
			else{
				System.out.println("error");
				System.exit(-1);
			}
		}
		return null;
	}
	
	public static void rtreebignode(){
		try{
			
			Vector leaf2 = new Vector();
			
			for(int k = 0; k < leaf.size(); k++){
				WNode n = (WNode)leaf.get(k);
				
				//System.out.println("leaf:" + k + ":" + n.obj.size());
				
				if(n.obj.size() > maxf){
					
					PropertySet ps2 = new PropertySet();

					Double f = new Double(0.7);
					ps2.setProperty("FillFactor", f);

					
					ps2.setProperty("IndexCapacity", maxf);
					ps2.setProperty("LeafCapacity", maxf);
						
					
					ps2.setProperty("Dimension", 2);

					ISpatialIndex tree = new RTree(ps2, new MemoryStorageManager());
					
					for(int i = 0; i < n.obj.size(); i++){
						int oid = (Integer)n.obj.get(i);
						WNode o = (WNode)objstore.get(oid);
						tree.insertData(null, o.mbr, o.id);
					}
					
					Vector sleaf = tree.getLeaf();
					
					leaf2.addAll(sleaf);
				}
				else
					leaf2.add(n);
			}
			
			leaf = (Vector)leaf2.clone();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void summarizeleafnode(){
		int word = 0;
		double area = 0;
		for(int i = 0; i < leaf.size(); i++){
			WNode n = (WNode)leaf.get(i);
			for(int j = 0; j < n.obj.size(); j++){
				int oid = (Integer)n.obj.get(j);
				WNode o = (WNode)objstore.get(oid);
				n.keyword.addAll(o.keyword);
				
				if(n.mbr == null){
					n.mbr = new Region(o.mbr);
				}
				else{
					n.mbr = n.mbr.combinedRegion(o.mbr);
				}
				
			}
			area += n.mbr.getArea();
			word += n.keyword.size();
			/*System.out.println("node:" + i);
			System.out.println("number of objects:" + n.obj.size());
			System.out.println("number of words:" + n.keyword.size());
			Iterator iter = n.keyword.iterator();
			while(iter.hasNext()){
				int w = (Integer)iter.next();
				System.out.print(w + " ");
			}
			System.out.print("\n");
			System.out.println("mbr area:" + n.mbr.getArea());*/
		}
		if(level == 0){
			System.out.println("average words:" + word/leaf.size());
			System.out.println("average mbr:" + area/leaf.size());
		}
	}
	
	static void writeRoot(){
		Node root = wtree.readNode(wtree.m_rootID);
		root.m_level = level;
		if(leaf.size() != 1){
			System.out.println("root error");
			System.exit(-1);
		}
		WNode kn = (WNode)leaf.get(0);
		for(int j = 0; j < kn.obj.size(); j++){
			int oid = (Integer)kn.obj.get(j);
			WNode knn = (WNode)objstore.get(oid);
			root.insertEntry(null, knn.mbr, knn.id);
		}
		wtree.writeNode(root);
	}
	
	static void writeNode(){
		for(int i = 0; i < leaf.size(); i++){
			WNode kn = (WNode)leaf.get(i);
			Node n;
			if(level == 0){
				n = new Leaf(wtree, -1);
			}
			else{
				n = new Index(wtree, -1, level);
			}
			for(int j = 0; j < kn.obj.size(); j++){
				int oid = (Integer)kn.obj.get(j);
				WNode knn = (WNode)objstore.get(oid);
				n.insertEntry(null, knn.mbr, knn.id);
			}
			wtree.writeNode(n);
			kn.id = n.m_identifier;
		}
	}
	
	static void clear(){
		objstore.clear();
		for(int i = 0; i < leaf.size(); i++){
			WNode kn = (WNode)leaf.get(i);
			objstore.put(kn.id, kn);
		}
		leaf.clear();
	}
}
