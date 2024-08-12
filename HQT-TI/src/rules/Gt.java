package rules;

import java.util.Vector;

public class Gt {

	int oid;
	float olat;
	float olng;
	public Vector Keywords;
	
	public Gt(int id, float lat, float lng){
		oid = id;
		olat = lat;
		olng = lng;
		Keywords = new Vector<Integer>();
	}
	

}
