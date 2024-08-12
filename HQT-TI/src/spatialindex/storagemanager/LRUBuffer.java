package spatialindex.storagemanager;

public class LRUBuffer extends Buffer{

    LRUCache<Integer, Entry> cache;

    public LRUBuffer(IStorageManager sm, int capacity, boolean bWriteThrough)
    {
        super(sm, capacity, bWriteThrough);
        cache = new LRUCache<Integer, Entry>(capacity);
    }

    public byte[] loadByteArray(final int id)
    {
        byte[] ret = null;
        Entry e = (Entry) cache.get(new Integer(id));

        if (e != null)
        {
            m_hits++;
            // System.out.println(1);
            ret = new byte[e.m_data.length];
            System.arraycopy(e.m_data, 0, ret, 0, e.m_data.length);
        }
        else
        {
            ret = m_storageManager.loadByteArray(id);
            e = new Entry(ret);
            cache.put(new Integer(id), e);
        }

        return ret;
    }



    void addEntry(int id, Entry e)
    {

    }

    void removeEntry()
    {

    }

    public int getIO(){
        return m_storageManager.getIO();
    }

}
