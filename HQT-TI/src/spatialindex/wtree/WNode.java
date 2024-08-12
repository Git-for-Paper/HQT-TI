package spatialindex.wtree;

import spatialindex.spatialindex.Region;

import java.util.HashSet;
import java.util.Vector;

public class WNode {

	public int id;
	public Vector obj;
	public Region mbr;
	public HashSet keyword;
	int wordlevel;
	
	public WNode(int wordlevel){
		obj = new Vector();
		keyword = new HashSet();
		mbr = null;
		this.wordlevel = wordlevel;
	}
	
	public WNode(int id, HashSet words){
		this.id = id;
		this.keyword = (HashSet)words.clone();
		obj = new Vector();
	}
}
