package spatialindex.spatialindex;


import spatialindex.rtree.LeafPostion;

public class NNEntry {


    public IEntry m_pEntry;
    public double m_minDist;
    public double m_maxDist;
    public double area;
    public LeafPostion leafPostion;

    public NNEntry(IEntry e, double f) {
        m_pEntry = e;
        m_minDist = f;

    }

    public NNEntry(IEntry e, double f, boolean tmp){
        m_pEntry = e;
        area = f;
    }

    public NNEntry(IEntry e, double f,LeafPostion lp){
        m_pEntry = e;
        m_minDist = f;
        leafPostion = lp;
    }

}