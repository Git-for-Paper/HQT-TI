package spatialindex.rtree;

public class LeafPostion {

    private int parentid;
    private int pos;
    private boolean isend;

    public LeafPostion(int parentid, int pos,boolean isend) {
        this.parentid = parentid;
        this.pos = pos;
        this.isend = isend;
    }

    public int getParentid() {
        return parentid;
    }

    public void setParentid(int parentid) {
        this.parentid = parentid;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public boolean isNext(LeafPostion lp1){
        if(this.parentid != lp1.parentid) {
            if(this.parentid+1 == lp1.parentid && this.isend==true && lp1.pos==0)
                return true;
            return false;
        }
        if(this.pos+1!=lp1.pos)
            return false;
        return true;
    }
}
