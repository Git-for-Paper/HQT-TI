package spatialindex.spatialindex;

import java.util.Hashtable;

public class RtreeEntry implements IEntry{

    int id;
    public boolean isLeafEntry;

    public int treeid;
    public int wordhit;
    public int parentid;

    public Hashtable distmap;

    IShape mbr;
    public double irscore;


    public RtreeEntry(int id, boolean f, int parentid){
        this.id = id;
        isLeafEntry = f;
        this.parentid = parentid;
        distmap = new Hashtable();
    }

    public RtreeEntry(int id, boolean f){
        this.id = id;
        isLeafEntry = f;
        distmap = new Hashtable();
    }
    public RtreeEntry(int id, double ir){
        this.id = id;
        this.irscore = ir;
    }
    public RtreeEntry(int id, Region mbr, double ir){
        this.id = id;
        this.mbr = new Region(mbr);
        this.irscore = ir;
    }
    public RtreeEntry(int id, Region mbr, boolean f){
        this.id = id;
        isLeafEntry = f;
        this.mbr = new Region(mbr);
        distmap = new Hashtable();
    }

    public int getIdentifier(){
        return id;
    }
    public IShape getShape(){
        return mbr;
    }

    public void setIdentifier(int id){
        this.id = id;
    }

    public void setMbr(IShape mbr) {
        this.mbr = mbr;
    }

    public int getParentid() {
        return parentid;
    }

    public void setParentid(int parentid) {
        this.parentid = parentid;
    }
}
