package spatialindex.spatialindex;

import java.util.Comparator;

public class NNEntryComparator1 implements Comparator {
    public int compare(Object o1, Object o2)
    {
        NNEntry n1 = (NNEntry) o1;
        NNEntry n2 = (NNEntry) o2;

        if (n1.m_pEntry.getParentid() < n2.m_pEntry.getParentid()) return -1;
        if (n1.m_pEntry.getParentid() > n2.m_pEntry.getParentid()) return 1;
        return 0;
    }
}
