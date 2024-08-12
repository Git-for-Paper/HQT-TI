package spatialindex.rtree;

public class Lenid {

    private int id;
    private int len;

    public Lenid(int id, int len) {
        this.id = id;
        this.len = len;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getLen() {
        return len;
    }

    public void setLen(int len) {
        this.len = len;
    }
}
