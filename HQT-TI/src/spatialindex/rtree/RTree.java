// Spatial Index Library
//
// Copyright (C) 2002  Navel Ltd.
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License aint with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Contact information:
//  Mailing address:
//    Marios Hadjieleftheriou
//    University of California, Riverside
//    Department of Computer Science
//    Surge Building, Room 310
//    Riverside, CA 92521
//
//  Email:
//    marioh@cs.ucr.edu

package spatialindex.rtree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

import algorithm.RQ.IqFile;
import algorithm.RQ.IterateRangeHeuristicIR;
import algorithm.RQ.IterateRangeInvertedR;
import algorithm.RQ.IterateRangeTireRtreer;
import jdbm.btree.BTree;
import mvs.rule;
import query.Group;
import query.Query;
import query.RegionQuery;
import spatialindex.spatialindex.*;
import spatialindex.storagemanager.*;
import storage.BitmapStore;
import storage.DocumentStore;
import storage.DocumentStoreWeight;
// import costmodel.LeafNodeAccessPro;
// import costmodel.NonLeafPostingList;
import documentindex.InvertedBitmap;
import documentindex.InvertedFile;
import documentindex.InvertedFileWeight;
import storage.posEntry;
import trie.Tire;
import trie.Trie;

public class RTree implements ISpatialIndex
{
    ///////////////////////////////////////
    public static long comparison = 0L;
    public static int numOfClusters;
    public static double alpha_dist;
    public static int bn = 0;
    public static long btime = 0;
    ///////////////////////////////////////
    RWLock m_rwLock;

    IStorageManager m_pStorageManager;

    public int m_rootID;
    public int m_headerID;

    int m_treeVariant;

    double m_fillFactor;

    int m_indexCapacity;

    int m_leafCapacity;

    int m_nearMinimumOverlapFactor;
    // The R*-Tree 'p' constant, for calculating nearly minimum overlap cost.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    // for Points and Rectangles, Section 4.1]

    double m_splitDistributionFactor;
    // The R*-Tree 'm' constant, for calculating spliting distributions.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    // for Points and Rectangles, Section 4.2]

    double m_reinsertFactor;
    // The R*-Tree 'p' constant, for removing entries at reinserts.
    // [Beckmann, Kriegel, Schneider, Seeger 'The R*-tree: An efficient and Robust Access Method
    //  for Points and Rectangles, Section 4.3]

    int m_dimension;

    Region m_infiniteRegion;

    Statistics m_stats;

    ArrayList m_writeNodeCommands = new ArrayList();
    ArrayList m_readNodeCommands = new ArrayList();
    ArrayList m_deleteNodeCommands = new ArrayList();

    public RTree(PropertySet ps, IStorageManager sm)
    {
        m_rwLock = new RWLock();
        m_pStorageManager = sm;
        m_rootID = IStorageManager.NewPage;
        m_headerID = IStorageManager.NewPage;
        m_treeVariant = SpatialIndex.RtreeVariantRstar;
        m_fillFactor = 0.7f;
        m_indexCapacity = 100;
        m_leafCapacity = 100;
        m_nearMinimumOverlapFactor = 32;
        m_splitDistributionFactor = 0.4f;
        m_reinsertFactor = 0.3f;
        m_dimension = 2;

        m_infiniteRegion = new Region();
        m_stats = new Statistics();

        Object var = ps.getProperty("IndexIdentifier");
        if (var != null)
        {
            if (! (var instanceof Integer)) throw new IllegalArgumentException("Property IndexIdentifier must an Integer");
            m_headerID = ((Integer) var).intValue();
            try
            {
                initOld(ps);
            }
            catch (IOException e)
            {
                System.err.println(e);
                throw new IllegalStateException("initOld failed with IOException");
            }
        }
        else
        {
            try
            {
                initNew(ps);
            }
            catch (IOException e)
            {
                System.err.println(e);
                throw new IllegalStateException("initNew failed with IOException");
            }
            Integer i = new Integer(m_headerID);
            ps.setProperty("IndexIdentifier", i);
        }
    }

    //
    // ISpatialIndex interface
    //

    public void insertData(final byte[] data, final IShape shape, int id)
    {
        if (shape.getDimension() != m_dimension) throw new IllegalArgumentException("insertData: Shape has the wrong number of dimensions.");

        m_rwLock.write_lock();

        try
        {
            Region mbr = shape.getMBR();

            byte[] buffer = null;

            if (data != null && data.length > 0)
            {
                buffer = new byte[data.length];
                System.arraycopy(data, 0, buffer, 0, data.length);
            }

            insertData_impl(buffer, mbr, id);
            // the buffer is stored in the tree. Do not delete here.
        }
        finally
        {
            m_rwLock.write_unlock();
        }
    }

    public boolean deleteData(final IShape shape, int id)
    {
        if (shape.getDimension() != m_dimension) throw new IllegalArgumentException("deleteData: Shape has the wrong number of dimensions.");

        m_rwLock.write_lock();

        try
        {
            Region mbr = shape.getMBR();
            return deleteData_impl(mbr, id);
        }
        finally
        {
            m_rwLock.write_unlock();
        }
    }

    public void containmentQuery(final IShape query, final IVisitor v)
    {
        if (query.getDimension() != m_dimension) throw new IllegalArgumentException("containmentQuery: Shape has the wrong number of dimensions.");
        rangeQuery(SpatialIndex.ContainmentQuery, query, v);
    }

    public void intersectionQuery(final IShape query, final IVisitor v)
    {
        if (query.getDimension() != m_dimension) throw new IllegalArgumentException("intersectionQuery: Shape has the wrong number of dimensions.");
        rangeQuery(SpatialIndex.IntersectionQuery, query, v);
    }

    public void pointLocationQuery(final IShape query, final IVisitor v)
    {
        if (query.getDimension() != m_dimension) throw new IllegalArgumentException("pointLocationQuery: Shape has the wrong number of dimensions.");

        Region r = null;
        if (query instanceof Point)
        {
            r = new Region((Point) query, (Point) query);
        }
        else if (query instanceof Region)
        {
            r = (Region) query;
        }
        else
        {
            throw new IllegalArgumentException("pointLocationQuery: IShape can be Point or Region only.");
        }

        rangeQuery(SpatialIndex.IntersectionQuery, r, v);
    }

    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v, final INearestNeighborComparator nnc)
    {
        if (query.getDimension() != m_dimension) throw new IllegalArgumentException("nearestNeighborQuery: Shape has the wrong number of dimensions.");

        m_rwLock.read_lock();

        try
        {
            // I need a priority queue here. It turns out that TreeSet sorts unique keys only and since I am
            // sorting according to distances, it is not assured that all distances will be unique. TreeMap
            // also sorts unique keys. Thus, I am simulating a priority queue using an ArrayList and binarySearch.
            ArrayList queue = new ArrayList();

            Node n = readNode(m_rootID);
            queue.add(new NNEntry(n, 0.0));

            int count = 0;
            double knearest = 0.0;

            while (queue.size() != 0)
            {
                NNEntry first = (NNEntry) queue.remove(0);

                if (first.m_pEntry instanceof Node)
                {
                    n = (Node) first.m_pEntry;
                    v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.m_children; cChild++)
                    {
                        IEntry e;

                        if (n.m_level == 0)
                        {
                            e = new Data(n.m_pData[cChild], n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                        }
                        else
                        {
                            e = (IEntry) readNode(n.m_pIdentifier[cChild]);
                        }

                        NNEntry e2 = new NNEntry(e, nnc.getMinimumDistance(query, e));

                        // Why don't I use a TreeSet here? See comment above...
                        int loc = Collections.binarySearch(queue, e2, new NNEntryComparator());
                        if (loc >= 0) queue.add(loc, e2);
                        else queue.add((-loc - 1), e2);
                    }
                }
                else
                {
                    // report all nearest neighbors with equal furthest distances.
                    // (neighbors can be more than k, if many happen to have the same
                    //  furthest distance).
                    if (count >= k && first.m_minDist > knearest) break;

                    v.visitData((IData) first.m_pEntry);
                    m_stats.m_queryResults++;
                    count++;
                    knearest = first.m_minDist;
                }
            }
        }
        finally
        {
            m_rwLock.read_unlock();
        }
    }

    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v)
    {
        if (query.getDimension() != m_dimension) throw new IllegalArgumentException("nearestNeighborQuery: Shape has the wrong number of dimensions.");
        NNComparator nnc = new NNComparator();
        nearestNeighborQuery(k, query, v, nnc);
    }

    public void queryStrategy(final IQueryStrategy qs)
    {
        m_rwLock.read_lock();

        int[] next = new int[] {m_rootID};

        try
        {
            while (true)
            {
                Node n = readNode(next[0]);
                boolean[] hasNext = new boolean[] {false};
                qs.getNextEntry(n, next, hasNext);
                if (hasNext[0] == false) break;
            }
        }
        finally
        {
            m_rwLock.read_unlock();
        }
    }

    public PropertySet getIndexProperties()
    {
        PropertySet pRet = new PropertySet();

        // dimension
        pRet.setProperty("Dimension", new Integer(m_dimension));

        // index capacity
        pRet.setProperty("IndexCapacity", new Integer(m_indexCapacity));

        // leaf capacity
        pRet.setProperty("LeafCapacity", new Integer(m_leafCapacity));

        // R-tree variant
        pRet.setProperty("TreeVariant", new Integer(m_treeVariant));

        // fill factor
        pRet.setProperty("FillFactor", new Double(m_fillFactor));

        // near minimum overlap factor
        pRet.setProperty("NearMinimumOverlapFactor", new Integer(m_nearMinimumOverlapFactor));

        // split distribution factor
        pRet.setProperty("SplitDistributionFactor", new Double(m_splitDistributionFactor));

        // reinsert factor
        pRet.setProperty("ReinsertFactor", new Double(m_reinsertFactor));

        return pRet;
    }

    public void addWriteNodeCommand(INodeCommand nc)
    {
        m_writeNodeCommands.add(nc);
    }

    public void addReadNodeCommand(INodeCommand nc)
    {
        m_readNodeCommands.add(nc);
    }

    public void addDeleteNodeCommand(INodeCommand nc)
    {
        m_deleteNodeCommands.add(nc);
    }

    public boolean isIndexValid()
    {
        boolean ret = true;
        Stack st = new Stack();
        Node root = readNode(m_rootID);

        if (root.m_level != m_stats.m_treeHeight - 1)
        {
            System.err.println("Invalid tree height");
            return false;
        }

        HashMap nodesInLevel = new HashMap();
        nodesInLevel.put(new Integer(root.m_level), new Integer(1));

        ValidateEntry e = new ValidateEntry(root.m_nodeMBR, root);
        st.push(e);

        while (! st.empty())
        {
            e = (ValidateEntry) st.pop();

            Region tmpRegion = (Region) m_infiniteRegion.clone();

            for (int cDim = 0; cDim < m_dimension; cDim++)
            {
                tmpRegion.m_pLow[cDim] = Double.POSITIVE_INFINITY;
                tmpRegion.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < e.m_pNode.m_children; cChild++)
                {
                    tmpRegion.m_pLow[cDim] = Math.min(tmpRegion.m_pLow[cDim], e.m_pNode.m_pMBR[cChild].m_pLow[cDim]);
                    tmpRegion.m_pHigh[cDim] = Math.max(tmpRegion.m_pHigh[cDim], e.m_pNode.m_pMBR[cChild].m_pHigh[cDim]);
                }
            }

            if (! (tmpRegion.equals(e.m_pNode.m_nodeMBR)))
            {
                System.err.println("Invalid parent information");
                ret = false;
            }
            else if (! (tmpRegion.equals(e.m_parentMBR)))
            {
                System.err.println("Error in parent");
                ret = false;
            }

            if (e.m_pNode.m_level != 0)
            {
                for (int cChild = 0; cChild < e.m_pNode.m_children; cChild++)
                {
                    ValidateEntry tmpEntry = new ValidateEntry(e.m_pNode.m_pMBR[cChild], readNode(e.m_pNode.m_pIdentifier[cChild]));

                    if (! nodesInLevel.containsKey(new Integer(tmpEntry.m_pNode.m_level)))
                    {
                        nodesInLevel.put(new Integer(tmpEntry.m_pNode.m_level), new Integer(1));
                    }
                    else
                    {
                        int i = ((Integer) nodesInLevel.get(new Integer(tmpEntry.m_pNode.m_level))).intValue();
                        nodesInLevel.put(new Integer(tmpEntry.m_pNode.m_level), new Integer(i + 1));
                    }

                    st.push(tmpEntry);
                }
            }
        }

        int nodes = 0;
        for (int cLevel = 0; cLevel < m_stats.m_treeHeight; cLevel++)
        {
            int i1 = ((Integer) nodesInLevel.get(new Integer(cLevel))).intValue();
            int i2 = ((Integer) m_stats.m_nodesInLevel.get(cLevel)).intValue();
            if (i1 != i2)
            {
                System.err.println("Invalid nodesInLevel information");
                ret = false;
            }

            nodes += i2;
        }

        if (nodes != m_stats.m_nodes)
        {
            System.err.println("Invalid number of nodes information");
            ret = false;
        }

        return ret;
    }

    public IStatistics getStatistics()
    {
        return (IStatistics) m_stats.clone();
    }

    public void flush() throws IllegalStateException
    {
        try
        {
            storeHeader();
            m_pStorageManager.flush();
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("flush failed with IOException");
        }
    }

    //
    // Internals
    //

    private void initNew(PropertySet ps) throws IOException
    {
        Object var;

        // tree variant.
        var = ps.getProperty("TreeVariant");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i != SpatialIndex.RtreeVariantLinear &&  i != SpatialIndex.RtreeVariantQuadratic && i != SpatialIndex.RtreeVariantRstar)
                    throw new IllegalArgumentException("Property TreeVariant not a valid variant");
                m_treeVariant = i;
            }
            else
            {
                throw new IllegalArgumentException("Property TreeVariant must be an Integer");
            }
        }

        // fill factor.
        var = ps.getProperty("FillFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property FillFactor must be in (0.0, 1.0)");
                m_fillFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property FillFactor must be a Double");
            }
        }

        // index capacity.
        var = ps.getProperty("IndexCapacity");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 3) throw new IllegalArgumentException("Property IndexCapacity must be >= 3");
                m_indexCapacity = i;
            }
            else
            {
                throw new IllegalArgumentException("Property IndexCapacity must be an Integer");
            }
        }

        // leaf capacity.
        var = ps.getProperty("LeafCapacity");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 3) throw new IllegalArgumentException("Property LeafCapacity must be >= 3");
                m_leafCapacity = i;
            }
            else
            {
                throw new IllegalArgumentException("Property LeafCapacity must be an Integer");
            }
        }

        // near minimum overlap factor.
        var = ps.getProperty("NearMinimumOverlapFactor");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 1 || i > m_indexCapacity || i > m_leafCapacity)
                    throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be less than both index and leaf capacities");
                m_nearMinimumOverlapFactor = i;
            }
            else
            {
                throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be an Integer");
            }
        }

        // split distribution factor.
        var = ps.getProperty("SplitDistributionFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property SplitDistributionFactor must be in (0.0, 1.0)");
                m_splitDistributionFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property SplitDistriburionFactor must be a Double");
            }
        }

        // reinsert factor.
        var = ps.getProperty("ReinsertFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property ReinsertFactor must be in (0.0, 1.0)");
                m_reinsertFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property ReinsertFactor must be a Double");
            }
        }

        // dimension
        var = ps.getProperty("Dimension");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i <= 1) throw new IllegalArgumentException("Property Dimension must be >= 1");
                m_dimension = i;
            }
            else
            {
                throw new IllegalArgumentException("Property Dimension must be an Integer");
            }
        }

        m_infiniteRegion.m_pLow = new double[m_dimension];
        m_infiniteRegion.m_pHigh = new double[m_dimension];

        for (int cDim = 0; cDim < m_dimension; cDim++)
        {
            m_infiniteRegion.m_pLow[cDim] = Double.POSITIVE_INFINITY;
            m_infiniteRegion.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;
        }

        m_stats.m_treeHeight = 1;
        m_stats.m_nodesInLevel.add(new Integer(0));

        Leaf root = new Leaf(this, -1);
        m_rootID = writeNode(root);

        storeHeader();
    }

    private void initOld(PropertySet ps) throws IOException
    {
        loadHeader();

        // only some of the properties may be changed.
        // the rest are just ignored.

        Object var;

        // tree variant.
        var = ps.getProperty("TreeVariant");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i != SpatialIndex.RtreeVariantLinear &&  i != SpatialIndex.RtreeVariantQuadratic && i != SpatialIndex.RtreeVariantRstar)
                    throw new IllegalArgumentException("Property TreeVariant not a valid variant");
                m_treeVariant = i;
            }
            else
            {
                throw new IllegalArgumentException("Property TreeVariant must be an Integer");
            }
        }

        // near minimum overlap factor.
        var = ps.getProperty("NearMinimumOverlapFactor");
        if (var != null)
        {
            if (var instanceof Integer)
            {
                int i = ((Integer) var).intValue();
                if (i < 1 || i > m_indexCapacity || i > m_leafCapacity)
                    throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be less than both index and leaf capacities");
                m_nearMinimumOverlapFactor = i;
            }
            else
            {
                throw new IllegalArgumentException("Property NearMinimumOverlapFactor must be an Integer");
            }
        }

        // split distribution factor.
        var = ps.getProperty("SplitDistributionFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property SplitDistributionFactor must be in (0.0, 1.0)");
                m_splitDistributionFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property SplitDistriburionFactor must be a Double");
            }
        }

        // reinsert factor.
        var = ps.getProperty("ReinsertFactor");
        if (var != null)
        {
            if (var instanceof Double)
            {
                double f = ((Double) var).doubleValue();
                if (f <= 0.0f || f >= 1.0f)
                    throw new IllegalArgumentException("Property ReinsertFactor must be in (0.0, 1.0)");
                m_reinsertFactor = f;
            }
            else
            {
                throw new IllegalArgumentException("Property ReinsertFactor must be a Double");
            }
        }

        m_infiniteRegion.m_pLow = new double[m_dimension];
        m_infiniteRegion.m_pHigh = new double[m_dimension];

        for (int cDim = 0; cDim < m_dimension; cDim++)
        {
            m_infiniteRegion.m_pLow[cDim] = Double.POSITIVE_INFINITY;
            m_infiniteRegion.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;
        }
    }

    private void storeHeader() throws IOException
    {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);

        ds.writeInt(m_rootID);
        ds.writeInt(m_treeVariant);
        ds.writeDouble(m_fillFactor);
        ds.writeInt(m_indexCapacity);
        ds.writeInt(m_leafCapacity);
        ds.writeInt(m_nearMinimumOverlapFactor);
        ds.writeDouble(m_splitDistributionFactor);
        ds.writeDouble(m_reinsertFactor);
        ds.writeInt(m_dimension);
        ds.writeLong(m_stats.m_nodes);
        ds.writeLong(m_stats.m_data);
        ds.writeInt(m_stats.m_treeHeight);

        for (int cLevel = 0; cLevel < m_stats.m_treeHeight; cLevel++)
        {
            ds.writeInt(((Integer) m_stats.m_nodesInLevel.get(cLevel)).intValue());
        }

        ds.flush();
        m_headerID = m_pStorageManager.storeByteArray(m_headerID, bs.toByteArray());
    }

    private void loadHeader() throws IOException
    {
        byte[] data = m_pStorageManager.loadByteArray(m_headerID);
        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));

        m_rootID = ds.readInt();
        m_treeVariant = ds.readInt();
        m_fillFactor = ds.readDouble();
        m_indexCapacity = ds.readInt();
        m_leafCapacity = ds.readInt();
        m_nearMinimumOverlapFactor = ds.readInt();
        m_splitDistributionFactor = ds.readDouble();
        m_reinsertFactor = ds.readDouble();
        m_dimension = ds.readInt();
        m_stats.m_nodes = ds.readLong();
        m_stats.m_data = ds.readLong();
        m_stats.m_treeHeight = ds.readInt();

        for (int cLevel = 0; cLevel < m_stats.m_treeHeight; cLevel++)
        {
            m_stats.m_nodesInLevel.add(new Integer(ds.readInt()));
        }
    }

    protected void insertData_impl(byte[] pData, Region mbr, int id)
    {
        assert mbr.getDimension() == m_dimension;

        boolean[] overflowTable;

        Stack pathBuffer = new Stack();

        Node root = readNode(m_rootID);

        overflowTable = new boolean[root.m_level];
        for (int cLevel = 0; cLevel < root.m_level; cLevel++) overflowTable[cLevel] = false;

        Node l = root.chooseSubtree(mbr, 0, pathBuffer);
        l.insertData(pData, mbr, id, pathBuffer, overflowTable);

        m_stats.m_data++;
    }

    protected void insertData_impl(byte[] pData, Region mbr, int id, int level, boolean[] overflowTable)
    {
        assert mbr.getDimension() == m_dimension;

        Stack pathBuffer = new Stack();

        Node root = readNode(m_rootID);
        Node n = root.chooseSubtree(mbr, level, pathBuffer);
        n.insertData(pData, mbr, id, pathBuffer, overflowTable);
    }

    protected boolean deleteData_impl(final Region mbr, int id)
    {
        assert mbr.getDimension() == m_dimension;

        boolean bRet = false;

        Stack pathBuffer = new Stack();

        Node root = readNode(m_rootID);
        Leaf l = root.findLeaf(mbr, id, pathBuffer);

        if (l != null)
        {
            l.deleteData(id, pathBuffer);
            m_stats.m_data--;
            bRet = true;
        }

        return bRet;
    }

    protected int writeNode(Node n) throws IllegalStateException
    {
        byte[] buffer = null;

        try
        {
            buffer = n.store();
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("writeNode failed with IOException");
        }

        int page;
        if (n.m_identifier < 0) page = IStorageManager.NewPage;
        else page = n.m_identifier;

        try
        {
            page = m_pStorageManager.storeByteArray(page, buffer);
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("writeNode failed with InvalidPageException");
        }

        if (n.m_identifier < 0)
        {
            n.m_identifier = page;
            m_stats.m_nodes++;
            int i = ((Integer) m_stats.m_nodesInLevel.get(n.m_level)).intValue();
            m_stats.m_nodesInLevel.set(n.m_level, new Integer(i + 1));
        }

        m_stats.m_writes++;

        for (int cIndex = 0; cIndex < m_writeNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) m_writeNodeCommands.get(cIndex)).execute(n);
        }

        return page;
    }

    protected Node readNode(int id)
    {
        byte[] buffer;
        DataInputStream ds = null;
        int nodeType = -1;
        Node n = null;

        try
        {
            buffer = m_pStorageManager.loadByteArray(id);
            ds = new DataInputStream(new ByteArrayInputStream(buffer));
            nodeType = ds.readInt();

            if (nodeType == SpatialIndex.PersistentIndex) n = new Index(this, -1, 0);
            else if (nodeType == SpatialIndex.PersistentLeaf) n = new Leaf(this, -1);
            else throw new IllegalStateException("readNode failed reading the correct node type information");

            n.m_pTree = this;
            n.m_identifier = id;
            n.load(buffer);

            m_stats.m_reads++;
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("readNode failed with InvalidPageException");
        }
        catch (IOException e)
        {
            System.err.println(e);
            throw new IllegalStateException("readNode failed with IOException");
        }

        for (int cIndex = 0; cIndex < m_readNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) m_readNodeCommands.get(cIndex)).execute(n);
        }

        return n;
    }

    protected void deleteNode(Node n)
    {
        try
        {
            m_pStorageManager.deleteByteArray(n.m_identifier);
        }
        catch (InvalidPageException e)
        {
            System.err.println(e);
            throw new IllegalStateException("deleteNode failed with InvalidPageException");
        }

        m_stats.m_nodes--;
        int i = ((Integer) m_stats.m_nodesInLevel.get(n.m_level)).intValue();
        m_stats.m_nodesInLevel.set(n.m_level, new Integer(i - 1));

        for (int cIndex = 0; cIndex < m_deleteNodeCommands.size(); cIndex++)
        {
            ((INodeCommand) m_deleteNodeCommands.get(cIndex)).execute(n);
        }
    }

    private void rangeQuery(int type, final IShape query, final IVisitor v)
    {
        m_rwLock.read_lock();

        try
        {
            Stack st = new Stack();
            Node root = readNode(m_rootID);

            if (root.m_children > 0 && query.intersects(root.m_nodeMBR)) st.push(root);

            while (! st.empty())
            {
                Node n = (Node) st.pop();

                if (n.m_level == 0)
                {
                    v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.m_children; cChild++)
                    {
                        boolean b;
                        if (type == SpatialIndex.ContainmentQuery) b = query.contains(n.m_pMBR[cChild]);
                        else b = query.intersects(n.m_pMBR[cChild]);

                        if (b)
                        {
                            Data data = new Data(n.m_pData[cChild], n.m_pMBR[cChild], n.m_pIdentifier[cChild]);
                            v.visitData(data);
                            m_stats.m_queryResults++;
                        }
                    }
                }
                else
                {
                    v.visitNode((INode) n);

                    for (int cChild = 0; cChild < n.m_children; cChild++)
                    {
                        if (query.intersects(n.m_pMBR[cChild]))
                        {
                            st.push(readNode(n.m_pIdentifier[cChild]));
                        }
                    }
                }
            }
        }
        finally
        {
            m_rwLock.read_unlock();
        }
    }

    public Vector rangeQuery1(Region region){
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        Vector<Integer> res = new Vector<>();
        while (queue.size() != 0){
            //System.out.println(1);
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                res.add(e.getIdentifier());
            }
            else{
                Node n = readNode(e.getIdentifier());
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }
                    if(region.intersects(n.m_pMBR[cChild])==true)
                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(region)));

                }
            }
        }
        return res;
    }


    public String toString()
    {
        String s = "Dimension: " + m_dimension + "\n"
                + "Fill factor: " + m_fillFactor + "\n"
                + "Index capacity: " + m_indexCapacity + "\n"
                + "Leaf capacity: " + m_leafCapacity + "\n";

        if (m_treeVariant == SpatialIndex.RtreeVariantRstar)
        {
            s += "Near minimum overlap factor: " + m_nearMinimumOverlapFactor + "\n"
                    + "Reinsert factor: " + m_reinsertFactor + "\n"
                    + "Split distribution factor: " + m_splitDistributionFactor + "\n";
        }

        s += "Utilization: " + 100 * m_stats.getNumberOfData() / (m_stats.getNumberOfNodesInLevel(0) * m_leafCapacity) + "%" + "\n"
                + m_stats;

        return s;
    }



    class NNComparator implements INearestNeighborComparator
    {
        public double getMinimumDistance(IShape query, IEntry e)
        {
            IShape s = e.getShape();
            return query.getMinimumDistance(s);
        }
    }

    class ValidateEntry
    {
        Region m_parentMBR;
        Node m_pNode;

        ValidateEntry(Region r, Node pNode) { m_parentMBR = r; m_pNode = pNode; }
    }

    class Data implements IData
    {
        int m_id;
        Region m_shape;
        byte[] m_pData;

        Data(byte[] pData, Region mbr, int id) { m_id = id; m_shape = mbr; m_pData = pData; }

        public int getIdentifier() { return m_id; }
        public IShape getShape() { return new Region(m_shape); }

        @Override
        public int getParentid() {
            return 0;
        }

        public byte[] getData()
        {
            byte[] data = new byte[m_pData.length];
            System.arraycopy(m_pData, 0, data, 0, m_pData.length);
            return data;
        }
    }

    public void IR(DocumentStore ds, InvertedFile invertedFile){

        try{
            Node n = readNode(m_rootID);

            IRpostOrder(ds, invertedFile, n);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private Vector IRpostOrder(DocumentStore ds, InvertedFile invertedFile, Node n){

        if(n.m_level == 0){
            try{

                invertedFile.create(n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    int docID = n.m_pIdentifier[child];

                    Vector document = ds.read(docID);
                    if(document == null){
                        System.out.println("Couldn't find document " + docID);
                        System.exit(-1);
                    }
                    invertedFile.addDocument(n.m_identifier, docID, document);

                }

                Vector pseudoDoc = invertedFile.store(n.m_identifier);

                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{

                invertedFile.create(n.m_identifier);
                System.out.println("processing index node " + n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    Vector pseudoDoc = IRpostOrder(ds, invertedFile, nn);
                    int docID = n.m_pIdentifier[child];
                    try{

                        if(pseudoDoc == null){
                            System.out.println("Couldn't find document " + docID);
                            System.exit(-1);

                        }
                        invertedFile.addDocument(n.m_identifier, docID, pseudoDoc);
                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }

                Vector pseudoDoc = invertedFile.store(n.m_identifier);


                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
    }

    public void IR(DocumentStoreWeight ds, InvertedFileWeight invertedFile, boolean termMBR){

        try{
            Node n = readNode(m_rootID);

            IRpostOrder(ds, invertedFile, n, termMBR);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private Vector IRpostOrder(DocumentStoreWeight ds, InvertedFileWeight invertedFile, Node n, boolean termMBR){

        if(n.m_level == 0){
            try{

                invertedFile.create(n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    int docID = n.m_pIdentifier[child];

                    Vector document = ds.read(docID % 1575149, true);
                    if(document == null){
                        System.out.println("Couldn't find document " + docID);
                        System.exit(-1);
                    }
                    invertedFile.addDocument(n.m_identifier, docID, document, n.m_pMBR[child], termMBR);
                }

                Vector pseudoDoc = invertedFile.store(n.m_identifier, n.m_level, termMBR);

                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{

                invertedFile.create(n.m_identifier);
                System.out.println("processing index node " + n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    Vector pseudoDoc = IRpostOrder(ds, invertedFile, nn, termMBR);
                    int docID = n.m_pIdentifier[child];
                    try{

                        if(pseudoDoc == null){
                            System.out.println("Couldn't find document " + docID);
                            System.exit(-1);

                        }
                        invertedFile.addDocument(n.m_identifier, docID, pseudoDoc, termMBR);
                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }

                Vector pseudoDoc = invertedFile.store(n.m_identifier, n.m_level, termMBR);


                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
    }

    public void IBR(DocumentStore ds, InvertedBitmap invertedBitmap){

        try{
            Node n = readNode(m_rootID);

            IBpostOrder(ds, invertedBitmap, n);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private Vector IBpostOrder(DocumentStore ds, InvertedBitmap invertedBitmap, Node n){

        if(n.m_level == 0){
            try{

                invertedBitmap.create(n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    int docID = n.m_pIdentifier[child];

                    Vector document = ds.read(docID);
                    if(document == null){
                        System.out.println("Couldn't find document " + docID);
                        System.exit(-1);
                    }
                    invertedBitmap.addDocument(n.m_identifier, child, document);

                }

                Vector pseudoDoc = invertedBitmap.store(n.m_identifier);

                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{

                invertedBitmap.create(n.m_identifier);
                System.out.println("processing index node " + n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    Vector pseudoDoc = IBpostOrder(ds, invertedBitmap, nn);
                    int docID = n.m_pIdentifier[child];
                    try{

                        if(pseudoDoc == null){
                            System.out.println("Couldn't find document " + docID);
                            System.exit(-1);

                        }
                        invertedBitmap.addDocument(n.m_identifier, child, pseudoDoc);
                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }

                Vector pseudoDoc = invertedBitmap.store(n.m_identifier);


                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
    }
    public void CIR(BTree clustertree, DocumentStoreWeight ds, InvertedFileWeight invertedFile, boolean termMBR) throws Exception{
        Node n = readNode(m_rootID);

        CIRpostOrder(clustertree, ds, invertedFile, n, termMBR);
    }
    private Vector[] CIRpostOrder(BTree clustertree, DocumentStoreWeight ds, InvertedFileWeight invertedFile, Node n, boolean termMBR) throws Exception{
        if(n.m_level == 0){
            invertedFile.create(n.m_identifier);

            int child;
            for(child = 0; child < n.m_children; child++){
                int docID = n.m_pIdentifier[child];

                Vector document = ds.read(docID % 1575149, true);
                if(document == null){
                    System.out.println("Couldn't find document " + docID);
                    System.exit(-1);
                }

                Object var = clustertree.find(docID % 1575149);
                if(var == null){
                    System.out.println("Couldn't find cluster " + docID);
                    System.exit(-1);
                }
                int cluster = (Integer)var;
                invertedFile.addDocument(n.m_identifier, docID, document, cluster, n.m_pMBR[child], termMBR);
            }
            Vector[] pseudoDoc = invertedFile.storeCIR(n.m_identifier, n.m_level, termMBR);

            return pseudoDoc;
        }
        else{
            invertedFile.create(n.m_identifier);
            System.out.println("processing index node " + n.m_identifier);
            int child;
            for(child = 0; child < n.m_children; child++){
                Node nn = readNode(n.m_pIdentifier[child]);
                Vector[] pseudoDoc = CIRpostOrder(clustertree, ds, invertedFile, nn, termMBR);
                int docID = n.m_pIdentifier[child];
                if(pseudoDoc == null){
                    System.out.println("Couldn't find document " + docID);
                    System.exit(-1);

                }
                for(int i = 0; i < pseudoDoc.length; i++){
                    if(pseudoDoc[i].size() == 0)
                        continue;
                    invertedFile.addDocument(n.m_identifier, docID, pseudoDoc[i], i, termMBR);
                }
            }
            Vector[] pseudoDoc = invertedFile.storeCIR(n.m_identifier, n.m_level, termMBR);

            return pseudoDoc;
        }
    }
    public void CIBR(BTree tree, DocumentStore ds, InvertedBitmap invertedBitmap){

        try{

            Node n = readNode(m_rootID);

            CIBRpostOrder(tree, ds, invertedBitmap, n);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private Vector[] CIBRpostOrder(BTree tree, DocumentStore ds, InvertedBitmap invertedBitmap, Node n){

        if(n.m_level == 0){
            try{

                invertedBitmap.create(n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    int docID = n.m_pIdentifier[child];

                    Vector document = ds.read(docID);
                    if(document == null){
                        System.out.println("Couldn't find document " + docID);
                        System.exit(-1);
                    }
                    Object var = tree.find(docID);
                    if(var == null){
                        System.out.println("Couldn't find cluster " + docID);
                        System.exit(-1);
                    }
                    int cluster = (Integer)var;

                    invertedBitmap.addDocument(n.m_identifier, child, document, cluster);

                }

                Vector[] pseudoDoc = invertedBitmap.storeCIBR(n.m_identifier);

                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{

                invertedBitmap.create(n.m_identifier);
                System.out.println("processing index node " + n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    Vector[] pseudoDoc = CIBRpostOrder(tree, ds, invertedBitmap, nn);
                    int docID = n.m_pIdentifier[child];
                    try{

                        if(pseudoDoc == null){
                            System.out.println("Couldn't find document " + docID);
                            System.exit(-1);

                        }
                        for(int i = 0; i < pseudoDoc.length; i++){
                            if(pseudoDoc[i].size() == 0)
                                continue;
                            invertedBitmap.addDocument(n.m_identifier, child, pseudoDoc[i], i);
                        }

                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }

                Vector[] pseudoDoc = invertedBitmap.storeCIBR(n.m_identifier);


                return pseudoDoc;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
    }
    public void IR2(DocumentStore ds, BitmapStore bs){

        try{

            Node n = readNode(m_rootID);


            IR2postOrder(ds, bs, n);

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private BitSet IR2postOrder(DocumentStore ds, BitmapStore bs, Node n){


        if(n.m_level == 0){
            try{

                BitSet bitmap = new BitSet(bs.bigword);

                int child;
                for(child = 0; child < n.m_children; child++){
                    int docID = n.m_pIdentifier[child];

                    Vector document = ds.read(docID);
                    if(document == null){
                        System.out.println("Couldn't find document " + docID);
                        System.exit(-1);
                    }
                    bs.addDocument(bitmap, document);
                    bs.store((docID+1)*(-1), document);
                }
                bs.flushbtree();
                bs.store(n.m_identifier, bitmap);

                return bitmap;
            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{

                BitSet bitmap = new BitSet(bs.bigword);
                System.out.println("processing index node " + n.m_identifier);

                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    BitSet pseudoBitmap = IR2postOrder(ds, bs, nn);
                    int docID = n.m_pIdentifier[child];
                    try{

                        if(pseudoBitmap == null){
                            System.out.println("Couldn't find bitmap " + docID);
                            System.exit(-1);

                        }
                        bs.addDocument(bitmap, pseudoBitmap);
                    }catch(Exception e){
                        e.printStackTrace();
                        System.exit(-1);
                    }

                }

                bs.store(n.m_identifier, bitmap);
                return bitmap;

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }
            return null;
        }
    }

    public void Iterate_IR(InvertedFile invertedFile, Query q, int topk) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                count++;
                System.out.println(e.getIdentifier() + "," + first.m_minDist);
                if(count == topk)
                    break;
            }
            else{
                Node n = readNode(e.getIdentifier());
                Hashtable filter = invertedFile.booleanfilter(n.m_identifier, q.qwords);
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    Object var = filter.get(n.m_pIdentifier[cChild]);
                    if(var == null)
                        continue;
                    else{
                        int hit = (Integer)var;
                        if(hit != q.qwords.size())
                            continue;
                    }

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }
                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));

                }
            }
        }
    }

    public void Iterate_IR_range(InvertedFile invertedFile, RegionQuery rq) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        	int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                //		count++;
                if(rq.qmbr.intersects(e.getShape())==false)
                    continue;
                count++;
                //System.out.println(e.getIdentifier());

            }
            else{
                Node n = readNode(e.getIdentifier());
                Hashtable filter = invertedFile.booleanfilter(n.m_identifier, rq.qwords);
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    Object var = filter.get(n.m_pIdentifier[cChild]);
                    if(var == null)
                        continue;
                    else{
                        int hit = (Integer)var;
                        if(hit != rq.qwords.size())
                            continue;
                    }

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    e.setMbr(n.m_pMBR[cChild]);
                    if(rq.qmbr.intersects(n.m_pMBR[cChild])==true)
                        queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(rq.qmbr)));

                }
            }
        }
        System.out.println(count);
    }
//	public void Iterate_IR(InvertedFile invertedFile, Query q, int topk) throws Exception
//	{
//		PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
//
//		RtreeEntry e = new RtreeEntry(m_rootID, false);
//		queue.add(new NNEntry(e, 0.0));
//		int count = 0;
//
//		while (queue.size() != 0){
//			NNEntry first = (NNEntry) queue.poll();
//			e = (RtreeEntry)first.m_pEntry;
//
//			if(e.isLeafEntry){
//				count++;
//				System.out.println(e.getIdentifier() + "," + first.m_minDist);
//				if(count == topk)
//					break;
//			}
//			else{
//				Node n = readNode(e.getIdentifier());
//				Hashtable filter = invertedFile.booleanfilter(n.m_identifier, q.qwords);
//				for (int cChild = 0; cChild < n.m_children; cChild++)
//				{
//					Object var = filter.get(n.m_pIdentifier[cChild]);
//					if(var == null)
//						continue;
//					else{
//						int hit = (Integer)var;
//						if(hit != q.qwords.size())
//							continue;
//					}
//
//					if (n.m_level == 0)
//					{
//						e = new RtreeEntry(n.m_pIdentifier[cChild], true);
//					}
//					else
//					{
//						e = new RtreeEntry(n.m_pIdentifier[cChild],false);
//					}
//
//					queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));
//
//				}
//			}
//		}
//	}

    public void Iterate_IBR(InvertedBitmap invertedBitmap, Query q, int topk) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                comparison++;
                count++;
                System.out.println(e.getIdentifier() + "," + first.m_minDist);
                if(count == topk)
                    break;
            }
            else{
                Node n = readNode(e.getIdentifier());
                BitSet filter = invertedBitmap.booleanfilter(n.m_identifier, q.qwords);

                //System.out.println("visiting node " + n.m_identifier);

                if(filter == null)
                    continue;

                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    comparison++;

                    boolean flag = filter.get(cChild);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));

                }
            }
        }
    }

    public void Iterate_IBR_range(InvertedBitmap invertedBitmap, RegionQuery rq) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                if(rq.qmbr.intersects(e.getShape())==false)
                    continue;
                comparison++;
                count++;
                //System.out.println(e.getIdentifier() + "," + first.m_minDist);
//				if(count == topk)
//					break;
            }
            else{
                Node n = readNode(e.getIdentifier());
                BitSet filter = invertedBitmap.booleanfilter(n.m_identifier, rq.qwords);

                //System.out.println("visiting node " + n.m_identifier);

                if(filter == null)
                    continue;

                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    comparison++;

                    boolean flag = filter.get(cChild);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    e.setMbr(n.m_pMBR[cChild]);
                    if(rq.qmbr.intersects(n.m_pMBR[cChild])==true)
                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(rq.qmbr)));

                }
            }
        }
        System.out.println(count);
    }

    public void Iterate_CIBR(InvertedBitmap invertedBitmap, Query q, int topk) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                count++;
                System.out.println(e.getIdentifier() + "," + first.m_minDist);
                if(count == topk)
                    break;
            }
            else{
                Node n = readNode(e.getIdentifier());
                BitSet filter = invertedBitmap.booleanfilterCIBR(n.m_identifier, q.qwords);
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    boolean flag = filter.get(cChild);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));

                }
            }
        }
    }

    public void Iterate_CIBR_range(InvertedBitmap invertedBitmap, RegionQuery rq) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

//            if(rq.qmbr.intersects((Region) e.getShape())== false)
//                continue;

            if(e.isLeafEntry){
                count++;
                //System.out.println(e.getIdentifier() + "," + first.m_minDist);
//				if(count == topk)
//					break;
            }
            else{
                Node n = readNode(e.getIdentifier());
                BitSet filter = invertedBitmap.booleanfilterCIBR(n.m_identifier, rq.qwords);
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    boolean flag = filter.get(cChild);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    e.setMbr(n.m_pMBR[cChild]);
                    if(rq.qmbr.intersects(n.m_pMBR[cChild])==true)
                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(rq.qmbr)));

                }
            }
        }
        System.out.println(count);
    }

    public void Iterate_IR2(BitmapStore bs, DocumentStore ds, Query q, int topk) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(10000, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                Vector document = ds.read(first.m_pEntry.getIdentifier());
                int match = 0;
                for(int i = 0; i < q.qwords.size(); i++){
                    int w = (Integer)q.qwords.get(i);
                    for(int j = 0; j < document.size(); j++){
                        int d = (Integer)document.get(j);
                        if(w == d){
                            match++;
                            break;
                        }
                    }
                }
                if(match == q.qwords.size()){
                    count++;
                    System.out.println(e.getIdentifier() + "," + first.m_minDist);
                    if(count == topk)
                        break;
                }

            }
            else{
                Node n = readNode(e.getIdentifier());

                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    boolean flag = bs.booleanfilter(n.m_pIdentifier[cChild], q.qwords, n.m_level);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));

                }
            }
        }
    }

    public void Iterate_IR2_range(BitmapStore bs, DocumentStore ds, RegionQuery rq) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                //System.out.println(1);
                if(rq.qmbr.intersects((Region) e.getShape())==false) continue;
                Vector document = ds.read(first.m_pEntry.getIdentifier());
                int match = 0;
                //System.out.println(document.size());
                for(int i = 0; i < rq.qwords.size(); i++){
                    int w = (Integer)rq.qwords.get(i);
                    for(int j = 0; j < document.size(); j++){
                        int d = (Integer)document.get(j);
                        if(w == d){
                            match++;
                            break;
                        }
                    }
                }
                if(match == rq.qwords.size()){
                    count++;

                    //System.out.println(e.getIdentifier());

                }

            }
            else{
                Node n = readNode(e.getIdentifier());

                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    boolean flag = bs.booleanfilter(n.m_pIdentifier[cChild], rq.qwords, n.m_level);
                    if(flag == false)
                        continue;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }

                    e.setMbr(n.m_pMBR[cChild]);
                    if(rq.qmbr.intersects(n.m_pMBR[cChild])==true)
                        queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(rq.qmbr)));

                }
            }
        }
        System.out.println(count);
    }

    public static int Iterate_MTR_range(String index_dir,RegionQuery rq) throws Exception{
        int IO = 0;
        Vector<Integer> fre = new Vector<>();
        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()+ Trie.node_num];
        for(int j = 0; j < rq.qwords.size(); j++){
            int word = (Integer)rq.qwords.get(j);
            if(Trie.tj[word]>=Trie.yue){
                fre.add(word);
                //System.out.println(word);
            }
            else{
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + word);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);

                dptree[j] = new RTree(dpps2, dpdiskfile);
            }
        }
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        for(int j = 0; j < rq.qwords.size(); j++){
            int word = (Integer)rq.qwords.get(j);
            if(Trie.tj[word]<Trie.yue)
                dptree[j].enqueueRoot(queue, j);
        }
        int pos = rq.qwords.size();
        fre.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(Trie.tj[o1]>Trie.tj[o2])
                    return -1;
                if(Trie.tj[o1]<Trie.tj[o2])
                    return 1;
                return 0;
            }
        });
        if(fre.size()>0){
            //System.out.println(fre);
            String filename="";
            for(int i=0;i<Trie.freset.size();i++){
                //System.out.println(Trie.freset.get(i)+" "+fre+" "+(Trie.freset.get(i).equals(fre.get(0))));
                if(Trie.freset.get(i).equals(fre.get(0))){

                    filename=Trie.tries.get(i).search(fre,0);
                    break;
                }
            }
            if(filename==null){
                System.out.println(0);
                return 0;
            }
            PropertySet dpps = new PropertySet();
            dpps.setProperty("FileName", index_dir + "/" + filename);
            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);
            dptree[pos] = new RTree(dpps2,dpdiskfile);
            dptree[pos].enqueueRoot(queue,pos);
            pos++;
        }
        int count = 0;
        Hashtable results = new Hashtable();
        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;
            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (rq.qmbr.intersects(e.getShape())==false) continue;
                //System.out.println(1);
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }

                if(fre.size()!=0&&e.treeid>=rq.qwords.size()){
                    e2.wordhit=e2.wordhit+fre.size()-1;

                }

                if(e2.wordhit == rq.qwords.size()){
                    count++;
                    System.out.println(e.getIdentifier());
                }
            }
            else{
                dptree[e.treeid].enqueueNode1(queue, e, rq.qmbr);
            }
        }
        for(int j = 0; j < pos; j++){
            if(j<rq.qwords.size()&&Trie.tj[(Integer) rq.qwords.get(j)]>=Trie.yue)
                continue;
            IO += dptree[j].getIO();
            //System.out.println(j+" "+IO);

            //dptree[j].flush();
        }
        System.out.println(count);
        return IO;
    }

    public static int Iterate_MTR(String index_dir,Query q,int topk) throws Exception{
        int IO = 0;
        Vector<Integer> fre = new Vector<>();
        ISpatialIndex[] dptree = new ISpatialIndex[q.qwords.size()+ Trie.node_num];
        for(int j = 0; j < q.qwords.size(); j++){
            int word = (Integer)q.qwords.get(j);
            if(Trie.tj[word]>=Trie.yue){
                fre.add(word);
                //System.out.println(word);
            }
            else{
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + word);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);

                dptree[j] = new RTree(dpps2, dpdiskfile);
            }
        }
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        for(int j = 0; j < q.qwords.size(); j++){
            int word = (Integer)q.qwords.get(j);
            if(Trie.tj[word]<Trie.yue)
                dptree[j].enqueueRoot(queue, j);
        }
        int pos = q.qwords.size();
        fre.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(Trie.tj[o1]>Trie.tj[o2])
                    return -1;
                if(Trie.tj[o1]<Trie.tj[o2])
                    return 1;
                return 0;
            }
        });
        if(fre.size()>0){
            //System.out.println(fre);
            String filename="";
            for(int i=0;i<Trie.freset.size();i++){
                //System.out.println(Trie.freset.get(i)+" "+fre+" "+(Trie.freset.get(i).equals(fre.get(0))));
                if(Trie.freset.get(i).equals(fre.get(0))){

                    filename=Trie.tries.get(i).search(fre,0);
                    break;
                }
            }
            if(filename==null){
                System.out.println(0);
                return 0;
            }
            PropertySet dpps = new PropertySet();
            dpps.setProperty("FileName", index_dir + "/" + filename);
            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);
            dptree[pos] = new RTree(dpps2,dpdiskfile);
            dptree[pos].enqueueRoot(queue,pos);
            pos++;
        }
        int count = 0;
        double knearest = 0;
        Hashtable results = new Hashtable();
        while (queue.size() != 0){
                NNEntry first = (NNEntry) queue.poll();
                RtreeEntry e = (RtreeEntry)first.m_pEntry;
            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (count >= topk && first.m_minDist > knearest) break;
                //System.out.println(1);
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }

                if(fre.size()!=0&&e.treeid>=q.qwords.size()){
                    e2.wordhit=e2.wordhit+fre.size()-1;

                }

                if(e2.wordhit == q.qwords.size()){
                    count++;
                    knearest = first.m_minDist;
                    System.out.println(e.getIdentifier() + "," + first.m_minDist);
                }
            }
            else{
                dptree[e.treeid].enqueueNode(queue, e, q);
            }
        }
        for(int j = 0; j < pos; j++){
            if(j<q.qwords.size()&&Trie.tj[(Integer) q.qwords.get(j)]>=Trie.yue)
                continue;
            IO += dptree[j].getIO();
            //System.out.println(j+" "+IO);

            //dptree[j].flush();
        }
        System.out.println(count);
        return IO;
    }

    public static int Iterate_rule_range(String index_dir,RegionQuery rq) throws Exception{
        int IO = 0;
        Vector<Integer> fre = new Vector<>();
        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()+ rule.rule_number];
        int pos=0;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        int[] bj = new int[10000005];
        for(int i=0;i<rule.vv.size();i++){
            int count = 0;
            for(int j=0;j<rq.qwords.size();j++){
                if(rq.qwords.get(j)==rule.vv.get(i).get(count)&&bj[rule.vv.get(i).get(count)]==0){
                    count++;
                }
                if(count==rule.vv.get(i).size()-2)
                    break;
            }
            //System.out.println(count);
            if(count==rule.vv.get(i).size()-2){
                //System.out.println("pass");
                StringBuilder filename = new StringBuilder();
                for(int j=0;j<rule.vv.get(i).size()-1;j++){
                    filename.append("_");
                    filename.append(rule.vv.get(i).get(j));
                    bj[rule.vv.get(i).get(j)]=1;
                }
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + filename);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);
                dptree[pos++] = new RTree(dpps2, dpdiskfile);

            }
        }
        for(int j=0;j<rq.qwords.size();j++){
            int word = (Integer) rq.qwords.get(j);
            if(bj[word]==0){
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + word);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);
                dptree[pos++] = new RTree(dpps2, dpdiskfile);
            }
        }
        for(int j=0;j<pos;j++){
            dptree[j].enqueueRoot(queue,j);
        }
        System.out.println(pos);
        int count = 0;
        Hashtable results = new Hashtable();
        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;
            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (rq.qmbr.intersects(e.getShape())==false) continue;
                //System.out.println(1);
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }

                if(e2.wordhit == pos){
                    count++;
                    //System.out.println(e.getIdentifier());
                }
            }
            else{
                dptree[e.treeid].enqueueNode1(queue, e, rq.qmbr);
            }
        }
        for(int j = 0; j < pos; j++){
            IO += dptree[j].getIO();
        }
        System.out.println(count);
        return IO;
    }

    public static boolean Search_Tire(Tire tire,Vector<Integer> wd,int i){
        if(tire.getKeyword()==wd.get(i)){
            if(i==0){
                //System.out.println(tire.getKeyword()+" "+wd.get(i));
                return true;
            }
            return Search_Tire(tire.getParent(),wd,i-1);
        }else{
            if(tire.getParent()==null||Tire.tj[tire.getKeyword()]>Tire.tj[wd.get(i)]){
                return false;
            }
            return Search_Tire(tire.getParent(),wd,i);
        }
    }

    public static int Iterate_TireRtree(String index_dir,Query q,int topk) throws Exception{
        int IO = 0;
        Vector<Integer> fre = new Vector<>();
        ISpatialIndex[] dptree = new ISpatialIndex[q.qwords.size()+Tire.node_num];
        for(int j = 0; j < q.qwords.size(); j++){
            int word = (Integer)q.qwords.get(j);
            if(Tire.tj[word]>=Tire.up_fre){
                fre.add(word);
                //System.out.println(word);
            }
            else{
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + word);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);

                dptree[j] = new RTree(dpps2, dpdiskfile);
            }
        }
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        for(int j = 0; j < q.qwords.size(); j++){
            int word = (Integer)q.qwords.get(j);
            if(Tire.tj[word]<Tire.up_fre)
                dptree[j].enqueueRoot(queue, j);
        }
        //System.out.println(fre.size());
        int pos = q.qwords.size();
        fre.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(Tire.tj[o1]>Tire.tj[o2])
                    return -1;
                if(Tire.tj[o1]<Tire.tj[o2])
                    return 1;
                return 0;
            }
        });

        if(fre.size()>0){
            int k = fre.get(fre.size()-1);
            //System.out.println(k+" "+Tire.vocabulary[k].size());
            for(int j=0;j<Tire.vocabulary[k].size();j++){
                Tire tire = Tire.vocabulary[k].get(j);
                boolean tmp = Search_Tire(tire,fre,fre.size()-1);
                //System.out.println(tmp+" "+pos);
                if(tmp==true){
                    PropertySet dpps = new PropertySet();
                    dpps.setProperty("FileName", Tire.vocabulary[k].get(j).getFilename());
                    IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                    //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                    PropertySet dpps2 = new PropertySet();
                    Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                    dpps2.setProperty("IndexIdentifier", dpi);
                    dptree[pos] = new RTree(dpps2,dpdiskfile);
                    dptree[pos].enqueueRoot(queue,pos);
                    pos++;
                }
            }
        }

        Hashtable results = new Hashtable();

        int count = 0;
        double knearest = 0.0;

        //System.out.println(fre.size()+" "+pos);
        while (queue.size() != 0)
        {
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;
            //System.out.println(e.getIdentifier()+" "+e.isLeafEntry);
            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (count >= topk && first.m_minDist > knearest) break;
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }
                //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                if(fre.size()!=0&&e.treeid>=q.qwords.size())
                    e2.wordhit=e2.wordhit+fre.size()-1;
                if(e2.wordhit == q.qwords.size()){
                    count++;
                    knearest = first.m_minDist;
                    System.out.println(e.getIdentifier() + "," + first.m_minDist);

                }
            }
            else{
                dptree[e.treeid].enqueueNode(queue, e, q);
            }
        }

        for(int j = 0; j < pos; j++){
            if(j<q.qwords.size()&&Tire.tj[(Integer) q.qwords.get(j)]>=Tire.up_fre)
                continue;
            IO += dptree[j].getIO();
            //System.out.println(j+" "+IO);

            //dptree[j].flush();
        }
        return IO;
    }

    public static int Iterate_TireRtree_range(String index_dir, RegionQuery rq) throws Exception {
        int IO = 0;
        int count = 0;
        Vector<Integer> fre = new Vector<>();
        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()+Tire.node_num];
        for(int j = 0; j < rq.qwords.size(); j++){
            int word = (Integer)rq.qwords.get(j);
            if(Tire.tj[word]>=Tire.up_fre){
                fre.add(word);
                //System.out.println(word);
            }
            else{
                PropertySet dpps = new PropertySet();

                dpps.setProperty("FileName", index_dir + "/" + word);

                IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                PropertySet dpps2 = new PropertySet();
                Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                dpps2.setProperty("IndexIdentifier", dpi);

                dptree[j] = new RTree(dpps2, dpdiskfile);
            }
        }
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        for(int j = 0; j < rq.qwords.size(); j++){
            int word = (Integer)rq.qwords.get(j);
            if(Tire.tj[word]<Tire.up_fre)
                dptree[j].enqueueRoot(queue, j);
        }
        //System.out.println(fre.size());
        int pos = rq.qwords.size();
        fre.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                if(Tire.tj[o1]>Tire.tj[o2])
                    return -1;
                if(Tire.tj[o1]<Tire.tj[o2])
                    return 1;
                return 0;
            }
        });
        if(fre.size()>0){
            IterateRangeTireRtreer.nuum++;
            int k = fre.get(fre.size()-1);
            //System.out.println(k+" "+Tire.vocabulary[k].size());
            for(int j=0;j<Tire.vocabulary[k].size();j++){
                Tire tire = Tire.vocabulary[k].get(j);
                boolean tmp = Search_Tire(tire,fre,fre.size()-1);
                //System.out.println(tmp+" "+pos);
                if(tmp==true){
                    PropertySet dpps = new PropertySet();
                    dpps.setProperty("FileName", Tire.vocabulary[k].get(j).getFilename());
                    IStorageManager dpdiskfile = new DiskStorageManager(dpps);

                    //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

                    PropertySet dpps2 = new PropertySet();
                    Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
                    dpps2.setProperty("IndexIdentifier", dpi);
                    dptree[pos] = new RTree(dpps2,dpdiskfile);
                    dptree[pos].enqueueRoot(queue,pos);
                    pos++;
                }
            }
        }
        Hashtable results = new Hashtable();
        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;
            //System.out.println(e.getIdentifier()+" "+e.isLeafEntry);
            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (rq.qmbr.intersects(e.getShape())==false) continue;
                //System.out.println(1);
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }
                //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                if(fre.size()!=0&&e.treeid>=rq.qwords.size())
                    e2.wordhit=e2.wordhit+fre.size()-1;
                if(e2.wordhit == rq.qwords.size()){
                    count++;
                    //System.out.println(e.getIdentifier());
                }
            }
            else{
                dptree[e.treeid].enqueueNode1(queue, e, rq.qmbr);
            }
        }
        for(int j = 0; j < pos; j++){
            if(j<rq.qwords.size()&&Tire.tj[(Integer) rq.qwords.get(j)]>=Tire.up_fre)
                continue;
            IO += dptree[j].getIO();
            //System.out.println(j+" "+IO);

            //dptree[j].flush();
        }
        System.out.println(count);
        return IO;
    }

    public static int Iterate_Cache(String index_dir, Vector<IqFile>[] iqs,int buffersize,int wnum,int[] qnum) throws Exception {
        int IO = 0;
        int ppp = 0;
        Hashtable[] results = new Hashtable[10005];
        Vector<Integer>[] res = new Vector[10005];
        for(int i=0;i<=10000;i++){
            results[i] = new Hashtable();
            res[i] = new Vector<>();
        }
        for(int i=1;i<=wnum;i++){
            PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator1());
            System.out.println(i+" "+iqs[i].size());
            if(iqs[i].size()==0) continue;
            PropertySet dpps = new PropertySet();
            IBuffer file;
            dpps.setProperty("FileName", index_dir + "/" + i);
            IStorageManager dpdiskfile = new DiskStorageManager(dpps);
            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1);
            dpps2.setProperty("IndexIdentifier", dpi);
            ISpatialIndex tree;
            if(buffersize>0){
                file = new LRUBuffer(dpdiskfile, buffersize, false);
                tree = new RTree(dpps2,file);
            }else {
                tree = new RTree(dpps2, dpdiskfile);
            }
            for(int j=0;j<iqs[i].size();j++){
                System.out.println(++ppp);
                Region mbr = iqs[i].get(j).region;
                int qid = iqs[i].get(j).qid;
                tree.enqueueRoot(queue,i);
                while (queue.size() != 0){
                    NNEntry first = (NNEntry) queue.poll();
                    RtreeEntry e = (RtreeEntry)first.m_pEntry;
                    if(e.isLeafEntry){
                        if(mbr.intersects(e.getShape())==false) continue;
                        RtreeEntry e2 = null;
                        if(results[qid].contains(e.getIdentifier())){
                            e2 = (RtreeEntry)results[qid].get(e.getIdentifier());
                            e2.wordhit++;
                            //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                        }
                        else{
                            e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                            e2.wordhit = 1;
                            results[qid].put(e2.getIdentifier(), e2);
                        }
                        if(e2.wordhit==qnum[qid]){
                            res[qid].add(e2.getIdentifier());
                        }
                    }else{
                        tree.enqueueNode1(queue,e,mbr);
                    }
                }
            }
//            IO+=tree.getIO();
//            tree.flush();
        }

        for(int i=1;i<=10000;i++){
            System.out.println("Query "+i);
            for(int j=0;j<res[i].size();j++){
                System.out.println(res[i].get(j));
            }
        }
        return IO;
    }

    public static int Iterate_InvertedRtree(String index_dir, Query q, int topk) throws Exception{
        int IO = 0;

        ISpatialIndex[] dptree = new ISpatialIndex[q.qwords.size()];
        for(int j = 0; j < q.qwords.size(); j++){

            int word = (Integer)q.qwords.get(j);

            //System.out.println("reading tree " + word);

            //load data point rtree
            PropertySet dpps = new PropertySet();

            dpps.setProperty("FileName", index_dir + "/" + word);

            //System.out.println(dpps.getProperty("Overwrite"));

            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);

            dptree[j] = new RTree(dpps2, dpdiskfile);

        }

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        for(int j = 0; j < dptree.length; j++){
            dptree[j].enqueueRoot(queue, j);
        }

        Hashtable results = new Hashtable();

        int count = 0;
        double knearest = 0.0;

        while (queue.size() != 0)
        {
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if (count >= topk && first.m_minDist > knearest) break;
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }

                if(e2.wordhit == q.qwords.size()){
                    count++;
                    knearest = first.m_minDist;
                    System.out.println(e.getIdentifier() + "," + first.m_minDist);

                }
            }
            else{
                dptree[e.treeid].enqueueNode(queue, e, q);
            }
        }

        for(int j = 0; j < dptree.length; j++){
            IO += dptree[j].getIO();
            //System.out.println(IO+" "+j);

        }
        return IO;
    }

    /*
    public static int Heuristic_InvertedRtree_range(String index_dir, RegionQuery rq) throws Exception{
        int IO = 0;

        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()];
        Area[] areas = new Area[rq.qwords.size()];
        for(int j = 0; j < rq.qwords.size(); j++){

            int word = (Integer)rq.qwords.get(j);

            //System.out.println("reading tree " + word);

            //load data point rtree
            PropertySet dpps = new PropertySet();

            dpps.setProperty("FileName", index_dir + "/" + word);

            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);

            dptree[j] = new RTree(dpps2, dpdiskfile);
            RtreeEntry entry = dptree[j].getroot(j);
            RTree rTree = (RTree) dptree[j];
            Node n = rTree.readNode(entry.getIdentifier());

            Region r = n.m_pMBR[0];
            for(int k=1;k<n.m_children;k++){
                r = r.combinedRegion(n.m_pMBR[k]);
            }
            areas[j] = new Area(j,r.getArea());
        }

        Arrays.sort(areas, new Comparator<Area>() {
            @Override
            public int compare(Area o1, Area o2) {
                if(o1.getArea()<o2.getArea())
                    return -1;
                else if(o1.getArea()>o2.getArea())
                    return 1;
                else
                return 0;
            }
        });

        int count = 0;
        Hashtable results = new Hashtable();
        Vector<Region> regions = new Vector<>();
        Region mbr = rq.qmbr;
        regions.add(mbr);
        for(int j=0;j<rq.qwords.size();j++){
            int ji = 0;
            //System.out.println(mbr.getArea());

            Region mbr1 = null;
            PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator1());
            dptree[areas[j].getId()].enqueueRoot(queue, areas[j].getId());
            while(queue.size()!=0){
                NNEntry first = (NNEntry) queue.poll();
                RtreeEntry e = (RtreeEntry)first.m_pEntry;
                if(e.isLeafEntry){
                    if(mbr.intersects(e.getShape())==false) continue;
                    RtreeEntry e2 = null;
                    if(results.containsKey(e.getIdentifier())){
                        e2 = (RtreeEntry)results.get(e.getIdentifier());
                        e2.wordhit++;
                        //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                    }
                    else{
                        e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                        e2.wordhit = 1;
                        results.put(e2.getIdentifier(), e2);
                    }
                    if(e2.wordhit == j+1){
                        if(mbr1 == null){
                            mbr1 = (Region) e.getShape();
                        }else{
                            mbr1 = mbr1.combinedRegion((Region) e.getShape());
                        }
                    }
                    if(e2.wordhit == rq.qwords.size()){
                        count++;
                        System.out.println(e.getIdentifier());
                    }
                }else{
                    dptree[e.treeid].enqueueNode1(queue, e , mbr);
                }
            }
            if(mbr1==null) break;
            else mbr=mbr1;
            System.out.println(mbr.getArea());
        }

        for(int j = 0; j < dptree.length; j++){
            IO += dptree[j].getIO();

            dptree[j].flush();
        }
        return IO;
    }
*/

    public static double getA(RTree rTree, int id){
        Node n = rTree.readNode(id);
        Region r = n.m_pMBR[0];
        if(r==null)
            return 0;
        for(int k=1;k<n.m_children;k++){
            r = r.combinedRegion(n.m_pMBR[k]);
        }
        return r.getArea();
    }

    public static int Heuristic_InvertedRtree_range(String index_dir, RegionQuery rq) throws Exception{
        int IO = 0;

        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()];
        Area[] areas = new Area[rq.qwords.size()];
        for(int j = 0; j < rq.qwords.size(); j++){

            int word = (Integer)rq.qwords.get(j);

            //System.out.println("reading tree " + word);

            //load data point rtree
            PropertySet dpps = new PropertySet();

            dpps.setProperty("FileName", index_dir + "/" + word);

            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);

            dptree[j] = new RTree(dpps2, dpdiskfile);

            areas[j] = new Area(j,IterateRangeHeuristicIR.areas[word]);
        }

        Arrays.sort(areas, new Comparator<Area>() {
            @Override
            public int compare(Area o1, Area o2) {
                if(o1.getArea()<o2.getArea())
                    return -1;
                else if(o1.getArea()>o2.getArea())
                    return 1;
                else
                    return 0;
            }
        });
        Hashtable results = new Hashtable();
        Vector<Region> vr1 = new Vector<>();
        vr1.add(rq.qmbr);
        int count =0;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator1());
        for(int j=0;j<rq.qwords.size();j++){
            Vector<Region> vr2 = new Vector<>();
            //LeafPostion now = new LeafPostion(0,0,false);
            int pid = 0;
            for(int k=0;k<vr1.size();k++){
                dptree[areas[j].getId()].enqueueRoot(queue, areas[j].getId());
                while(queue.size()!=0){
                    IterateRangeHeuristicIR.ssum++;
                    NNEntry first = (NNEntry) queue.poll();
                    RtreeEntry e = (RtreeEntry)first.m_pEntry;
                    if(e.isLeafEntry){
                        if(vr1.get(k).intersects(e.getShape())==false) continue;
                        RtreeEntry e2 = null;
                        if(results.containsKey(e.getIdentifier())){
                            e2 = (RtreeEntry)results.get(e.getIdentifier());
                            e2.wordhit++;
                            //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                        }
                        else{
                            e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                            e2.wordhit = 1;
                            results.put(e2.getIdentifier(), e2);
                        }
                        if(e2.wordhit==j+1){
                            if(vr2.size()==0){
                                vr2.add((Region) e.getShape());
                            }else{
                                if(pid == e.getParentid()){
                                    vr2.set(vr2.size()-1,vr2.get(vr2.size()-1).combinedRegion((Region) e.getShape()));
                                }else{
                                    vr2.add((Region) e.getShape());
                                }
                            }
                            pid = e.getParentid();
                        }
                        if(e2.wordhit==rq.qwords.size()){
                            count++;
                            //System.out.println(e.getIdentifier());
                        }
                    }else{
                        dptree[e.treeid].enqueueNode2(queue, e , vr1.get(k));
                    }
                }
            }

            vr1 = vr2;
            System.out.println(vr1.size());


        }
        System.out.println(count);
        for(int j = 0; j < dptree.length; j++){
            IO += dptree[j].getIO();

//            dptree[j].flush();
        }
        return IO;
    }

    public void enqueueNode2(PriorityQueue queue,RtreeEntry entry,Region region){
        Node n = readNode(entry.getIdentifier());
        for (int cChild = 0; cChild < n.m_children; cChild++)
        {
            RtreeEntry e;

            if (n.m_level == 0)
            {
//                LeafPostion lp;
//                if(cChild == n.m_children-1)
//                    lp = new LeafPostion(entry.getIdentifier(),cChild,true);
//                else
//                    lp = new LeafPostion(entry.getIdentifier(),cChild,false);
                e = new RtreeEntry(n.m_pIdentifier[cChild], true, entry.getIdentifier());
                e.treeid = entry.treeid;
                e.setMbr(n.m_pMBR[cChild]);
                if(n.m_pMBR[cChild].intersects(region)==true)
                    queue.add(new NNEntry(e, e.getParentid()));
            }
            else
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], false, entry.getIdentifier());
                e.treeid = entry.treeid;
                e.setMbr(n.m_pMBR[cChild]);
                if(n.m_pMBR[cChild].intersects(region)==true)
                    queue.add(new NNEntry(e, e.getParentid()));
            }
        }
    }

//    public static int Heuristic_InvertedRtree_range(String index_dir, RegionQuery rq) throws Exception{
//        int IO = 0;
//
//        int[] len = IterateRangeHeuristicIR.len;
//        ISpatialIndex[] dptree = new ISpatialIndex[rq.qwords.size()];
//        Lenid[] lenids = new Lenid[rq.qwords.size()];
//        for(int j = 0; j < rq.qwords.size(); j++){
//
//            int word = (Integer)rq.qwords.get(j);
//
//            //System.out.println("reading tree " + word);
//
//            //load data point rtree
//            PropertySet dpps = new PropertySet();
//
//            dpps.setProperty("FileName", index_dir + "/" + word);
//
//            IStorageManager dpdiskfile = new DiskStorageManager(dpps);
//
//            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);
//
//            PropertySet dpps2 = new PropertySet();
//            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
//            dpps2.setProperty("IndexIdentifier", dpi);
//
//            dptree[j] = new RTree(dpps2, dpdiskfile);
//            RtreeEntry entry = dptree[j].getroot(j);
//            RTree rTree = (RTree) dptree[j];
//            Node n = rTree.readNode(entry.getIdentifier());
//            lenids[j] = new Lenid(j,len[(Integer)rq.qwords.get(j)]);
//        }
//        Arrays.sort(lenids, new Comparator<Lenid>() {
//            @Override
//            public int compare(Lenid o1, Lenid o2) {
//                if(o1.getLen()<o2.getLen())
//                    return -1;
//                else if(o1.getLen()>o2.getLen())
//                    return 1;
//                else
//                return 0;
//            }
//        });
//        Region mbr = rq.qmbr;
//        Hashtable results = new Hashtable();
//        Vector<Region> regions = new Vector<>();
//        Vector<Integer> res = new Vector<>();
//        int j=0;
//        for(;j<rq.qwords.size();j++){
//            regions = new Vector<>();
//            res = new Vector<>();
//            PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator1());
//            dptree[lenids[j].getId()].enqueueRoot(queue, lenids[j].getId());
//            while (queue.size()!=0){
//                NNEntry first = (NNEntry) queue.poll();
//                RtreeEntry e = (RtreeEntry)first.m_pEntry;
//                if(e.isLeafEntry){
//                    if(mbr.intersects(e.getShape()) == false) continue;
//                    RtreeEntry e2 = null;
//                    if(results.containsKey(e.getIdentifier())){
//                        e2 = (RtreeEntry)results.get(e.getIdentifier());
//                        e2.wordhit++;
//                    }else{
//                        e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
//                        e2.wordhit = 1;
//                        results.put(e2.getIdentifier(), e2);
//                    }
//                    if(e2.wordhit==j+1){
//                        regions.add((Region) e.getShape());
//                        res.add(e.getIdentifier());
//                    }
//                }else{
//                    dptree[e.treeid].enqueueNode1(queue, e , mbr);
//                }
//            }
//            if(j==3)
//                IterateRangeHeuristicIR.c++;
//            //System.out.println(regions.size());
//            if(regions.size()<100)
//                break;
//        }
//        for(;j<rq.qwords.size();j++){
//            RtreeEntry entry = ((RTree) dptree[lenids[j].getId()]).getroot(lenids[j].getId());
//            for(int k=0;k<regions.size();k++){
//                //System.out.println(regions.get(k));
//                boolean jg = ((RTree) dptree[lenids[j].getId()]).isleafexist(regions.get(k),entry);
//                if(jg==false){
//                    regions.remove(k);
//                    res.remove(k);
//                    k--;
//                }
//            }
//        }
//        for(int k=0;k<res.size();k++){
//            System.out.println(res.get(k));
//        }
//
//        for(int k = 0; k < dptree.length; k++){
//            IO += dptree[k].getIO();
//
//            dptree[k].flush();
//        }
//        return IO;
//    }

    public RtreeEntry getroot(int treeid){
        RtreeEntry entry = new RtreeEntry(m_rootID,false);
        entry.treeid = treeid;
        return entry;
    }

    public boolean isleafexist(Region leafregion, RtreeEntry entry){
        Node n = readNode(entry.getIdentifier());

        boolean jg = false;
        for (int cChild = 0; cChild < n.m_children; cChild++)
        {
            RtreeEntry e;

            if (n.m_level == 0)
            {
                if(n.m_pMBR[cChild].intersects(leafregion)){
                    //System.out.println(n.m_pMBR[cChild].intersects(leafregion));
                    //System.out.println(n.m_pMBR[cChild]+"/"+leafregion);
                    return true;
                }

            }
            else
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], false);
                e.treeid = entry.treeid;
                e.setMbr(n.m_pMBR[cChild]);
                if(n.m_pMBR[cChild].intersects(leafregion))
                    jg = jg || isleafexist(leafregion,e);
            }
        }
        return jg;
    }

    public static int Iterate_InvertedRtree_range(String index_dir, RegionQuery q) throws Exception{
        int IO = 0;

        ISpatialIndex[] dptree = new ISpatialIndex[q.qwords.size()];
        for(int j = 0; j < q.qwords.size(); j++){

            int word = (Integer)q.qwords.get(j);

            //System.out.println("reading tree " + word);

            //load data point rtree
            PropertySet dpps = new PropertySet();

            dpps.setProperty("FileName", index_dir + "/" + word);

            IStorageManager dpdiskfile = new DiskStorageManager(dpps);

            //IBuffer dpfile = new RandomEvictionsBuffer(dpdiskfile, 10, false);

            PropertySet dpps2 = new PropertySet();
            Integer dpi = new Integer(1); // INDEX_IDENTIFIER_GOES_HERE (suppose I know that in this case it is equal to 1);
            dpps2.setProperty("IndexIdentifier", dpi);

            dptree[j] = new RTree(dpps2, dpdiskfile);

        }

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());


        Hashtable results = new Hashtable();

        int count = 0;
        double knearest = 0.0;
        for(int j = 0; j < dptree.length; j++){
            dptree[j].enqueueRoot(queue, j);
        }
        while (queue.size() != 0)
        {
            IterateRangeInvertedR.ssum++;
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry)
            {
                // report all nearest neighbors with equal furthest distances.
                // (neighbors can be more than k, if many happen to have the same
                //  furthest distance).
                if(q.qmbr.intersects(e.getShape())==false)
                    continue;
                RtreeEntry e2 = null;
                if(results.containsKey(e.getIdentifier())){
                    e2 = (RtreeEntry)results.get(e.getIdentifier());
                    e2.wordhit++;
                    //System.out.println(e2.getIdentifier()+" "+e2.wordhit);
                }
                else{
                    e2 = new RtreeEntry(e.getIdentifier(), e.isLeafEntry);
                    e2.wordhit = 1;
                    results.put(e2.getIdentifier(), e2);
                }

                if(e2.wordhit == q.qwords.size()){
                    count++;
                    knearest = first.m_minDist;
                    //System.out.println(e.getIdentifier());
                }
            }
            else{
                dptree[e.treeid].enqueueNode1(queue, e ,q.qmbr);
            }
        }
        //System.out.println(mbr.toString());

        for(int j = 0; j < dptree.length; j++){
            IO += dptree[j].getIO();

            //dptree[j].flush();
        }
        System.out.println(count);
        return IO;
    }


    public void Group_IBR(InvertedBitmap invertedBitmap, Group group, int topk, PriorityQueue[] result){

        double maxdist = Double.POSITIVE_INFINITY;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        NNEntry ne = new NNEntry(e, 0.0);
        for(int j = 0; j < group.queries.size(); j++){
            e.distmap.put(j, 0.0);
        }
        queue.add(ne);
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry firste = (RtreeEntry)first.m_pEntry;

            //rule 1
            if(first.m_minDist >= maxdist)
                break;

            group.words.clear();

            double mD = Double.POSITIVE_INFINITY;
            Iterator iter = firste.distmap.keySet().iterator();
            while(iter.hasNext()){
                comparison++;
                int pos = (Integer)iter.next();
                double d = (Double)firste.distmap.get(pos);
                Query q = (Query)group.queries.get(pos);

                if(d < q.distance_bound){
                    group.words.addAll(q.qwords);
                    mD = Math.min(mD, d);
                }
            }
            //rule 3
            if(group.words.size() == 0){
                //pruned++;
                continue;
            }

            if(mD != first.m_minDist){
                first.m_minDist = mD;
                queue.add(first);
                continue;
            }

            Node n = readNode(firste.getIdentifier());

            Hashtable keywordfilter = invertedBitmap.groupbooleanfilter(n.m_identifier, group);

            //System.out.println("visiting node " + n.m_identifier);
            //System.out.println("relevant to " + firste.distmap.keySet().toString());

            if(keywordfilter == null)
                continue;
            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                Object var = keywordfilter.get(cChild);
                if(var == null)
                    continue;

                HashSet bs = (HashSet)var;
                bs.retainAll(group.words);
                //rule 1
                if(bs.size() >= group.minSize){
                    double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                    //rule 2
                    if(dist < maxdist){
                        bs = (HashSet)var;

                        //non-leaf node
                        if (n.m_level != 0){
                            RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],false);
                            double minD = Double.POSITIVE_INFINITY;
                            iter = firste.distmap.keySet().iterator();

                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();
                                //for(int j = 0; j < group.queries.size(); j++){

                                Query q = (Query)group.queries.get(pos);
                                //Query q = (Query)group.queries.get(j);

                                comparison++;

                                if(bs.containsAll(q.qwords)){
                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    //rule 3
                                    if(d >= q.distance_bound)
                                        continue;

                                    minD = Math.min(minD, d);

                                    e2.distmap.put(pos, d);
                                    //e2.distmap.put(j, d);
                                }
                            }
                            if(minD == Double.POSITIVE_INFINITY)
                                continue;

                            ne = new NNEntry(e2, minD);
                            queue.add(ne);
                        }

                        //leaf node
                        else{

                            boolean flag = false;
                            iter = firste.distmap.keySet().iterator();

                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();
                                //for(int j = 0; j < group.queries.size(); j++){

                                Query q = (Query)group.queries.get(pos);
                                //Query q = (Query)group.queries.get(j);

                                comparison++;

                                if(bs.containsAll(q.qwords)){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],true);
                                    ne = new NNEntry(e2, d);

                                    result[pos].add(ne);
                                    result[pos].poll();
                                    ne = (NNEntry)result[pos].peek();

                                    //result[j].add(ne);
                                    //result[j].poll();
                                    //ne = (NNEntry)result[j].peek();

                                    if(q.distance_bound == maxdist)
                                        flag = true;
                                    q.distance_bound = ne.m_minDist;
                                }

                            }
                            if(flag){
                                double findmaxdist = Double.NEGATIVE_INFINITY;
                                for(int j = 0; j < group.queries.size(); j++){
                                    Query tmpq = (Query)group.queries.get(j);
                                    findmaxdist = Math.max(findmaxdist, tmpq.distance_bound);
                                }
                                maxdist = findmaxdist;
                            }
                        }
                    }
                }
                //else pruned++;
            }

        }
    }
    public void Group_IBRV2(InvertedBitmap invertedBitmap, Group group, int topk, PriorityQueue[] result){
        ///////////////////////////////////////////
        Vector columns = new Vector();
        ///////////////////////////////////////////
        double maxdist = Double.POSITIVE_INFINITY;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        NNEntry ne = new NNEntry(e, 0.0);
        for(int j = 0; j < group.queries.size(); j++){
            e.distmap.put(j, 0.0);
            columns.add(new BitSet(m_indexCapacity));
        }
        queue.add(ne);
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry firste = (RtreeEntry)first.m_pEntry;

            //rule 1
            if(first.m_minDist >= maxdist)
                break;

            group.words.clear();

            double mD = Double.POSITIVE_INFINITY;
            Iterator iter = firste.distmap.keySet().iterator();
            while(iter.hasNext()){
                comparison++;
                int pos = (Integer)iter.next();
                double d = (Double)firste.distmap.get(pos);
                Query q = (Query)group.queries.get(pos);

                if(d < q.distance_bound){
                    group.words.addAll(q.qwords);
                    mD = Math.min(mD, d);
                }
            }
            //rule 3
            if(group.words.size() == 0){
                //pruned++;
                continue;
            }

            if(mD != first.m_minDist){
                first.m_minDist = mD;
                queue.add(first);
                continue;
            }

            Node n = readNode(firste.getIdentifier());

            //non-leaf node
            if (n.m_level != 0){
                Hashtable keywordfilter = invertedBitmap.groupbooleanfilter(n.m_identifier, group, columns, n.m_children);

                if(keywordfilter == null)
                    continue;
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    Object var = keywordfilter.get(cChild);
                    if(var == null)
                        continue;

                    //rule 1
                    if((Boolean)var){
                        double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                        //rule 2
                        if(dist < maxdist){
                            RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],false);
                            double minD = Double.POSITIVE_INFINITY;
                            iter = firste.distmap.keySet().iterator();

                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();
                                Query q = (Query)group.queries.get(pos);
                                comparison++;

                                double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                //rule 3
                                if(d >= q.distance_bound)
                                    continue;

                                minD = Math.min(minD, d);

                                e2.distmap.put(pos, d);

                            }
                            if(minD == Double.POSITIVE_INFINITY)
                                continue;

                            ne = new NNEntry(e2, minD);
                            queue.add(ne);

                        }
                    }
                }
            }
            //leaf node
            else{
                Hashtable keywordfilter = invertedBitmap.groupbooleanfilter(n.m_identifier, group);
                if(keywordfilter == null)
                    continue;
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    Object var = keywordfilter.get(cChild);
                    if(var == null)
                        continue;

                    HashSet bs = (HashSet)var;
                    bs.retainAll(group.words);
                    //rule 1
                    if(bs.size() >= group.minSize){
                        double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                        //rule 2
                        if(dist < maxdist){
                            bs = (HashSet)var;
                            boolean flag = false;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();
                                Query q = (Query)group.queries.get(pos);
                                comparison++;

                                if(bs.containsAll(q.qwords)){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],true);
                                    ne = new NNEntry(e2, d);

                                    result[pos].add(ne);
                                    result[pos].poll();
                                    ne = (NNEntry)result[pos].peek();

                                    if(q.distance_bound == maxdist)
                                        flag = true;
                                    q.distance_bound = ne.m_minDist;
                                }

                            }
                            if(flag){
                                double findmaxdist = Double.NEGATIVE_INFINITY;
                                for(int j = 0; j < group.queries.size(); j++){
                                    Query tmpq = (Query)group.queries.get(j);
                                    findmaxdist = Math.max(findmaxdist, tmpq.distance_bound);
                                }
                                maxdist = findmaxdist;
                            }
                        }
                    }
                }
            }
        }
    }
    public void Group_CIBR(InvertedBitmap invertedBitmap, Group group, int topk, PriorityQueue[] result){
        double maxdist = Double.POSITIVE_INFINITY;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        NNEntry ne = new NNEntry(e, 0.0);
        for(int j = 0; j < group.queries.size(); j++){

            e.distmap.put(j, 0.0);
        }
        queue.add(ne);
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry firste = (RtreeEntry)first.m_pEntry;

            //rule 1
            if(first.m_minDist >= maxdist)
                break;

            group.words.clear();

            double mD = Double.POSITIVE_INFINITY;
            Iterator iter = firste.distmap.keySet().iterator();
            while(iter.hasNext()){
                comparison++;
                int pos = (Integer)iter.next();
                double d = (Double)firste.distmap.get(pos);
                Query q = (Query)group.queries.get(pos);

                if(d < q.distance_bound){
                    group.words.addAll(q.qwords);
                    mD = Math.min(mD, d);
                }
            }
            //rule 3
            if(group.words.size() == 0){
                //pruned++;
                continue;
            }

            if(mD != first.m_minDist){
                first.m_minDist = mD;
                queue.add(first);
                continue;
            }

            Node n = readNode(firste.getIdentifier());
            Hashtable keywordfilter = invertedBitmap.groupbooleanfilterCIBR(n.m_identifier, group);

            //System.out.println("visiting node " + n.m_identifier);
            //System.out.println("relevant to " + firste.distmap.keySet().toString());

            if(keywordfilter == null)
                continue;
            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                Object var = keywordfilter.get(cChild);
                if(var == null)
                    continue;

                Vector bsvector = (Vector)var;
                boolean flag = false;
                for(int i = 0; i < bsvector.size(); i++){
                    HashSet bs = (HashSet)bsvector.get(i);
                    bs.retainAll(group.words);
                    if(bs.size() >= group.minSize){
                        flag = true;
                        break;
                    }
                }

                //rule 1
                if(flag){
                    double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                    //rule 2
                    if(dist < maxdist){

                        //non-leaf node
                        if (n.m_level != 0){
                            RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],false);
                            double minD = Double.POSITIVE_INFINITY;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();

                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                flag = false;
                                for(int i = 0; i < bsvector.size(); i++){
                                    HashSet bs = (HashSet)bsvector.get(i);
                                    if(bs.containsAll(q.qwords)){
                                        flag = true;
                                        break;
                                    }
                                }
                                if(flag){
                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    //rule 3
                                    if(d >= q.distance_bound)
                                        continue;

                                    minD = Math.min(minD, d);
                                    e2.distmap.put(pos, d);

                                }
                            }
                            if(minD == Double.POSITIVE_INFINITY)
                                continue;

                            ne = new NNEntry(e2, minD);
                            queue.add(ne);
                        }

                        //leaf node
                        else{

                            flag = false;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();

                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                flag = false;
                                for(int i = 0; i < bsvector.size(); i++){
                                    HashSet bs = (HashSet)bsvector.get(i);
                                    if(bs.containsAll(q.qwords)){
                                        flag = true;
                                        break;
                                    }
                                }
                                if(flag){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],true);
                                    ne = new NNEntry(e2, d);
                                    result[pos].add(ne);
                                    result[pos].poll();
                                    ne = (NNEntry)result[pos].peek();

                                    if(q.distance_bound == maxdist)
                                        flag = true;
                                    q.distance_bound = ne.m_minDist;
                                }

                            }
                            if(flag){
                                double findmaxdist = Double.NEGATIVE_INFINITY;
                                for(int j = 0; j < group.queries.size(); j++){
                                    Query tmpq = (Query)group.queries.get(j);
                                    findmaxdist = Math.max(findmaxdist, tmpq.distance_bound);
                                }
                                maxdist = findmaxdist;
                            }
                        }
                    }
                }
                //else pruned++;
            }

        }
    }
    public void Group_IR(InvertedFile invertedFile, Group group, int topk, PriorityQueue[] result){
        double maxdist = Double.POSITIVE_INFINITY;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        NNEntry ne = new NNEntry(e, 0.0);
        for(int j = 0; j < group.queries.size(); j++){

            e.distmap.put(j, 0.0);
        }
        queue.add(ne);
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry firste = (RtreeEntry)first.m_pEntry;

            //rule 1
            if(first.m_minDist >= maxdist)
                break;

            group.words.clear();

            double mD = Double.POSITIVE_INFINITY;
            Iterator iter = firste.distmap.keySet().iterator();
            while(iter.hasNext()){
                comparison++;
                int pos = (Integer)iter.next();
                double d = (Double)firste.distmap.get(pos);
                Query q = (Query)group.queries.get(pos);

                if(d < q.distance_bound){
                    group.words.addAll(q.qwords);
                    mD = Math.min(mD, d);
                }
            }
            //rule 3
            if(group.words.size() == 0){
                //pruned++;
                continue;
            }

            if(mD != first.m_minDist){
                first.m_minDist = mD;
                queue.add(first);
                continue;
            }

            Node n = readNode(firste.getIdentifier());

            Hashtable filter = invertedFile.groupbooleanfilter(n.m_identifier, group);

            //System.out.println("visiting node " + n.m_identifier);
            //System.out.println("relevant to " + firste.distmap.keySet().toString());

            if(filter == null)
                continue;
            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                Object var = filter.get(n.m_pIdentifier[cChild]);
                if(var == null)
                    continue;

                HashSet bs = (HashSet)var;
                bs.retainAll(group.words);
                //rule 1
                if(bs.size() >= group.minSize){
                    double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                    //rule 2
                    if(dist < maxdist){
                        bs = (HashSet)var;

                        //non-leaf node
                        if (n.m_level != 0){
                            RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],false);
                            double minD = Double.POSITIVE_INFINITY;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();
                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                if(bs.containsAll(q.qwords)){
                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    //rule 3
                                    if(d >= q.distance_bound)
                                        continue;

                                    minD = Math.min(minD, d);
                                    e2.distmap.put(pos, d);

                                }
                            }
                            if(minD == Double.POSITIVE_INFINITY)
                                continue;

                            ne = new NNEntry(e2, minD);
                            queue.add(ne);
                        }

                        //leaf node
                        else{

                            boolean flag = false;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();

                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                if(bs.containsAll(q.qwords)){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],true);
                                    ne = new NNEntry(e2, d);
                                    result[pos].add(ne);
                                    result[pos].poll();
                                    ne = (NNEntry)result[pos].peek();


                                    if(q.distance_bound == maxdist)
                                        flag = true;
                                    q.distance_bound = ne.m_minDist;
                                }

                            }
                            if(flag){
                                double findmaxdist = Double.NEGATIVE_INFINITY;
                                for(int j = 0; j < group.queries.size(); j++){
                                    Query tmpq = (Query)group.queries.get(j);
                                    findmaxdist = Math.max(findmaxdist, tmpq.distance_bound);
                                }
                                maxdist = findmaxdist;
                            }
                        }
                    }
                }
                //else pruned++;
            }

        }
    }
    public void Group_IR2(BitmapStore bs, DocumentStore ds, Group group, int topk, PriorityQueue[] result) throws Exception
    {
        double maxdist = Double.POSITIVE_INFINITY;
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        NNEntry ne = new NNEntry(e, 0.0);
        for(int j = 0; j < group.queries.size(); j++){

            e.distmap.put(j, 0.0);
        }
        queue.add(ne);
        int count = 0;

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            RtreeEntry firste = (RtreeEntry)first.m_pEntry;

            //rule 1
            if(first.m_minDist >= maxdist)
                break;

            group.words.clear();

            double mD = Double.POSITIVE_INFINITY;
            Iterator iter = firste.distmap.keySet().iterator();
            while(iter.hasNext()){
                comparison++;
                int pos = (Integer)iter.next();
                double d = (Double)firste.distmap.get(pos);
                Query q = (Query)group.queries.get(pos);

                if(d < q.distance_bound){
                    group.words.addAll(q.qwords);
                    mD = Math.min(mD, d);
                }
            }
            //rule 3
            if(group.words.size() == 0){
                //pruned++;
                continue;
            }

            if(mD != first.m_minDist){
                first.m_minDist = mD;
                queue.add(first);
                continue;
            }

            Node n = readNode(firste.getIdentifier());

            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                int id = -1;
                if(n.m_level == 0)
                    id = (n.m_pIdentifier[cChild]+1) *(-1);
                else
                    id = n.m_pIdentifier[cChild];

                BitSet bitmap = bs.read(id);

                //rule 1
                if(bitmap.cardinality() >= group.minSize)
                {
                    double dist = group.mbr.getMinimumDistance(n.m_pMBR[cChild]);
                    //rule 2
                    if(dist < maxdist){

                        //non-leaf node
                        if (n.m_level != 0){
                            RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],false);
                            double minD = Double.POSITIVE_INFINITY;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();

                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                boolean flag = true;
                                for(int i = 0; i < q.qwords.size(); i++)
                                {
                                    int word = (Integer)q.qwords.get(i);
                                    int pos2 = word % bs.bigword;
                                    if(!bitmap.get(pos2)){
                                        flag = false;
                                        break;
                                    }
                                }
                                if(flag){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    minD = Math.min(minD, d);
                                    e2.distmap.put(pos, d);

                                }

                            }
                            if(minD == Double.POSITIVE_INFINITY)
                                continue;

                            ne = new NNEntry(e2, minD);
                            queue.add(ne);
                        }

                        //leaf node
                        else{
                            Vector document = ds.read(n.m_pIdentifier[cChild]);
                            boolean flag = false;
                            iter = firste.distmap.keySet().iterator();
                            while(iter.hasNext()){
                                int pos = (Integer)iter.next();

                                Query q = (Query)group.queries.get(pos);

                                comparison++;

                                int match = 0;
                                for(int i = 0; i < q.qwords.size(); i++){
                                    int word = (Integer)q.qwords.get(i);
                                    for(int k = 0; k < document.size(); k++){
                                        int d = (Integer)document.get(k);
                                        if(word == d){
                                            match++;
                                            break;
                                        }
                                    }
                                }

                                if(match == q.qwords.size()){

                                    double d = n.m_pMBR[cChild].getMinimumDistance(q.qpoint);
                                    RtreeEntry e2 = new RtreeEntry(n.m_pIdentifier[cChild],true);
                                    ne = new NNEntry(e2, d);
                                    result[pos].add(ne);
                                    result[pos].poll();
                                    ne = (NNEntry)result[pos].peek();

                                    if(q.distance_bound == maxdist)
                                        flag = true;
                                    q.distance_bound = ne.m_minDist;
                                }

                            }
                            if(flag){
                                double findmaxdist = Double.NEGATIVE_INFINITY;
                                for(int j = 0; j < group.queries.size(); j++){
                                    Query tmpq = (Query)group.queries.get(j);
                                    findmaxdist = Math.max(findmaxdist, tmpq.distance_bound);
                                }
                                maxdist = findmaxdist;
                            }
                        }
                    }
                }
                //else pruned++;
            }

        }
    }

    public void enqueueRoot(PriorityQueue queue, int treeid){

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        e.treeid = treeid;
        queue.add(new NNEntry(e, 0.0));
    }

    public void enqueueNode(PriorityQueue queue, RtreeEntry entry, Query q){

        Node n = readNode(entry.getIdentifier());

        for (int cChild = 0; cChild < n.m_children; cChild++)
        {
            RtreeEntry e;

            if (n.m_level == 0)
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], true);

            }
            else
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], false);
            }
            e.treeid = entry.treeid;
            queue.add(new NNEntry(e, n.m_pMBR[cChild].getMinimumDistance(q.qpoint)));
        }
    }

    public void enqueueNode1(PriorityQueue queue, RtreeEntry entry, Region region){
        Node n = readNode(entry.getIdentifier());

        for (int cChild = 0; cChild < n.m_children; cChild++)
        {
            RtreeEntry e;

            if (n.m_level == 0)
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], true, entry.getIdentifier());

            }
            else
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], false, entry.getIdentifier());
            }
            e.treeid = entry.treeid;
            e.setMbr(n.m_pMBR[cChild]);
            if(n.m_pMBR[cChild].intersects(region)==true)
                queue.add(new NNEntry(e, n.m_pMBR[cChild].getArea()));
        }
    }

    public int getIO(){
        return m_pStorageManager.getIO();
    }

    public Vector getLeaf(){
        Node n = readNode(m_rootID);
        Vector sleaf = new Vector();
        postOrder(n, sleaf);
        return sleaf;
    }
    void postOrder(Node n, Vector sleaf){
        if(n.m_level == 0){
            try{

                sleaf.add(n);

            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }

        }
        else{
//////////////////////////////////////////////////////////////////////////////////////////
            try{


                int child;
                for(child = 0; child < n.m_children; child++){
                    Node nn = readNode(n.m_pIdentifier[child]);
                    postOrder(nn, sleaf);

                }
            }catch(Exception e){
                e.printStackTrace();
                System.exit(-1);
            }

        }
    }

    public static void print(PriorityQueue result){
        Vector r = new Vector();
        while(!result.isEmpty()){
            NNEntry e = (NNEntry)result.poll();
            r.add(e);
        }
        for(int j = r.size()-1; j>=0; j--){
            NNEntry e = (NNEntry)r.get(j);
            if(e.m_pEntry != null)
                System.out.println(e.m_pEntry.getIdentifier() + "," + e.m_minDist);

        }
    }
/*
    public int RkT(InvertedFileWeight invertedFile, RegionQuery q, int topk, int filtering) throws Exception
    {
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        double threshold = Double.POSITIVE_INFINITY;
        Vector cand = new Vector();
        ArrayList ranking = new ArrayList();

        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;
            if(first.m_minDist > threshold){

                break;
            }

            if(e.isLeafEntry){
                cand.add(first);
                if(cand.size() >= topk){
                    ranking.clear();
                    for(int i = 0; i < cand.size(); i++){
                        NNEntry c = (NNEntry)cand.get(i);

                        int loc = Collections.binarySearch(ranking, c.m_maxDist);
                        if(loc >= 0)
                            ranking.add(loc, c.m_maxDist);
                        else
                            ranking.add((-loc-1), c.m_maxDist);
                    }
                    threshold = (Double)ranking.get(topk-1);

                    for(int i = 0; i < cand.size(); i++){
                        NNEntry c = (NNEntry)cand.get(i);
                        if(c.m_minDist > threshold){
                            cand.remove(c);
                            i--;
                        }
                    }
                }


            }
            else{
                Node n = readNode(e.getIdentifier());
                Hashtable filter = invertedFile.IRScore(n.m_identifier, q.qwords);
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    double irscore;
                    Object var = filter.get(n.m_pIdentifier[cChild]);
                    if(var == null)
                        irscore = Double.MIN_VALUE;
                        //continue;
                    else
                        irscore = (Double)var;


                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild], n.m_pMBR[cChild], true);
                        e.irscore = irscore;
                        Point p = new Point(n.m_pMBR[cChild].m_pLow);

                        double mind = combinedScore(q.qmbr.getMinimumDistance(p), irscore);

                        //System.out.println("mind:" + q.qmbr.getMinimumDistance(p));

                        NNEntry nne = new NNEntry(e, mind);
                        nne.m_maxDist = combinedScore(q.qmbr.getMaximumDistance(p), irscore);

                        //System.out.println("maxd:" + q.qmbr.getMaximumDistance(p));

                        queue.add(nne);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                        e.irscore = irscore;
                        double mind = combinedScore(n.m_pMBR[cChild].getMinimumDistance(q.qmbr), irscore);
                        NNEntry nne = new NNEntry(e, mind);
                        queue.add(nne);
                    }

                }
            }
        }
        if(filtering == 1){
            SimpleRangeKnnManager res = new SimpleRangeKnnManager(q.qmbr, topk);

            for(int i = 0; i < cand.size(); i++){
                NNEntry c = (NNEntry)cand.get(i);

                res.UpdateBoundary(c);
            }
            res.FilterStep();
            return res.combineResults();
        }
        else{
            for(int i = 0; i < cand.size(); i++){
                NNEntry c = (NNEntry)cand.get(i);
                System.out.println(c.m_pEntry.getIdentifier() + "," + c.m_minDist + ":" + c.m_maxDist);

            }
            return cand.size();
        }
    }

    public SimpleRangeKnnManager RangekNN(InvertedFileWeight invertedFile, RegionQuery q, int topk, int filtering, String type) throws Exception
    {
        long start = System.currentTimeMillis();
        SimpleRangeKnnManager res = new SimpleRangeKnnManager(q.qmbr, topk);

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        double[] low = new double[2];
        double[] high = new double[2];
        low[0] = 0; high[0] = 1;
        low[0] = 0; high[1] = 1;
        Region rootmbr = new Region(low, high);
        RtreeEntry e = new RtreeEntry(m_rootID, rootmbr, 1);
        queue.add(new NNEntry(e, 0.0));

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(res.isNonLeafEntryPruned(first))
                break;

            Node n = readNode(e.getIdentifier());
            Hashtable filter;
            if(RTree.numOfClusters != 0){
                filter = invertedFile.CIRScore(n.m_identifier, q, n.m_pIdentifier, n.m_level, n.m_children, type);
            }
            else{
                filter = invertedFile.IRScore(n.m_identifier, q.qwords);
            }
            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                double irscore;
                Object var = filter.get(n.m_pIdentifier[cChild]);
                if(var == null)
                    //irscore = Double.MIN_VALUE;
                    continue;
                else
                    irscore = (Double)var;

                e = new RtreeEntry(n.m_pIdentifier[cChild], n.m_pMBR[cChild], irscore);
                if (n.m_level == 0){
                    res.LeafEntryUpdate(e);
                }
                else{
                    //double mind = combinedScore(n.m_pMBR[cChild].getMinimumDistance(q.qmbr), irscore);
                    NNEntry nne = new NNEntry(e, irscore);
                    queue.add(nne);
                }
            }
        }
        long end = System.currentTimeMillis();
        btime += (end-start);
        int n = res.countExternalResult();
        System.out.println("number of results before filtering: " + n);
        bn += n;
        if(filtering == 1){
            res.FilterStep();
            //res.FilterInsideObj();
        }
        return res;
    }

    public SimpleRangeKnnManager RangeRIF(InvertedFileWeight invertedFile, RegionQuery q, int topk) throws Exception
    {
        long start = System.currentTimeMillis();

        Hashtable rankingtable = invertedFile.IRScore(0, q.qwords);
        RtreeEntry maxtr = getMaxTR(rankingtable);

        SimpleRangeKnnManager res = new SimpleRangeKnnManager(q.qmbr, topk);
        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());

        double[] low = new double[2];
        double[] high = new double[2];
        low[0] = 0; high[0] = 1;
        low[0] = 0; high[1] = 1;
        Region rootmbr = new Region(low, high);
        RtreeEntry e = new RtreeEntry(m_rootID, rootmbr, 1);
        queue.add(new NNEntry(e, 0.0));

        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;
            first.m_minDist = combinedScore(first.m_minDist, maxtr.irscore);
            if(res.isNonLeafEntryPruned(first))
                break;

            Node n = readNode(e.getIdentifier());

            for (int cChild = 0; cChild < n.m_children; cChild++)
            {
                e = new RtreeEntry(n.m_pIdentifier[cChild], n.m_pMBR[cChild], 0);

                if (n.m_level == 0){
                    double irscore;
                    Object var = rankingtable.get(n.m_pIdentifier[cChild]);
                    if(var == null)
                        //irscore = Double.MIN_VALUE;
                        continue;
                    else
                        irscore = (Double)var;

                    e.irscore = irscore;
                    res.LeafEntryUpdate(e);

                    rankingtable.remove(e.getIdentifier());
                    if(e.getIdentifier() == maxtr.getIdentifier())
                        maxtr = getMaxTR(rankingtable);
                }
                else{
                    double mind = n.m_pMBR[cChild].getMinimumDistance(q.qmbr);
                    NNEntry nne = new NNEntry(e, mind);
                    queue.add(nne);
                }
            }
        }
        long end = System.currentTimeMillis();
        btime += (end-start);
        int n = res.countExternalResult();
        System.out.println("number of results before filtering: " + n);
        bn += n;

        return res;
    }

 */

    public static double combinedScore(double spatial, double ir){
        return (alpha_dist*spatial+(1-alpha_dist)*(1-ir));
    }

    private RtreeEntry getMaxTR(Hashtable rankingtable){
        RtreeEntry maxtr = new RtreeEntry(-1, Double.MIN_VALUE);
        if(rankingtable.isEmpty()){
            return maxtr;
        }
        else{
            Iterator iter = rankingtable.keySet().iterator();
            while(iter.hasNext()){
                int id = (Integer)iter.next();
                double tr = (Double)rankingtable.get(id);
                if(tr > maxtr.irscore){
                    maxtr.setIdentifier(id);
                    maxtr.irscore = tr;
                }
            }
            return maxtr;
        }
    }
    public void clusterMBR(BTree clustertree, DocumentStore cm) throws Exception{
        Node n = readNode(m_rootID);
        clusterMBRpostOrder(clustertree, cm, n);
    }

    private Region[] clusterMBRpostOrder(BTree clustertree, DocumentStore cm, Node n) throws Exception{
        if(n.m_level == 0){
            Region[] mbrs = new Region[numOfClusters];
            int child;
            for(child = 0; child < n.m_children; child++){
                int docID = n.m_pIdentifier[child];

                Object var = clustertree.find(docID % 1575149);
                //System.out.println(((posEntry)var).getLen());
                if(var == null){
                    System.out.println("Couldn't find cluster " + docID);
                    System.exit(-1);
                }
                int cluster = (int) ((posEntry)var).getPos()%numOfClusters;
                if(mbrs[cluster] == null){
                    mbrs[cluster] = new Region(n.m_pMBR[child]);
                }
                else{
                    Region.combinedRegion(mbrs[cluster], n.m_pMBR[child]);
                }
            }
            //cm.insertMBR(n.m_identifier, mbrs);

            return mbrs;
        }
        else{
            Region[] mbrs = new Region[numOfClusters];
            Vector vmbr = new Vector();
            System.out.println("processing index node " + n.m_identifier);
            int child;
            for(child = 0; child < n.m_children; child++){
                Node nn = readNode(n.m_pIdentifier[child]);
                Region[] childmbrs = clusterMBRpostOrder(clustertree, cm, nn);
                int docID = n.m_pIdentifier[child];
                if(childmbrs == null){
                    System.out.println("Couldn't find mbrs " + docID);
                    System.exit(-1);

                }
                for(int i = 0; i < mbrs.length; i++){
                    if(mbrs[i] == null){
                        if(childmbrs[i] != null)
                            mbrs[i] = new Region(childmbrs[i]);
                    }
                    else{
                        if(childmbrs[i] != null)
                            Region.combinedRegion(mbrs[i], childmbrs[i]);
                    }
                }
                vmbr.add(childmbrs);
            }
            cm.insertDocument(n.m_identifier, vmbr);

            return mbrs;
        }
    }

    public void lkt(InvertedFileWeight invertedFile, Query q, int topk, int cluster, DocumentStore cm, String type) throws Exception{

        PriorityQueue queue = new PriorityQueue(100, new NNEntryComparator());
        RtreeEntry e = new RtreeEntry(m_rootID, false);
        queue.add(new NNEntry(e, 0.0));
        int count = 0;
        double knearest = 0.0;
        while (queue.size() != 0){
            NNEntry first = (NNEntry) queue.poll();
            e = (RtreeEntry)first.m_pEntry;

            if(e.isLeafEntry){
                if(count >= topk && first.m_minDist > knearest)
                    break;

                count++;
                System.out.println(e.getIdentifier() + "," + first.m_minDist);
                knearest = first.m_minDist;
            }
            else{
                Node n = readNode(e.getIdentifier());
                //System.out.println("node:" + n.m_identifier + "," + first.m_minDist);
                Hashtable filter;
                if(cluster != 0){
                    filter = invertedFile.CIRScore(n.m_identifier, q, cm, n.m_pIdentifier, n.m_level, n.m_children, type);
                }
                else{
                    filter = invertedFile.IRScore(n.m_identifier, q, n.m_level, type);
                }
                for (int cChild = 0; cChild < n.m_children; cChild++)
                {
                    double irscore;
                    Object var = filter.get(n.m_pIdentifier[cChild]);
                    if(var == null)
                        //irscore = Double.MIN_VALUE;
                        continue;
                    else
                        irscore = (Double)var;

                    if (n.m_level == 0)
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],true);
                    }
                    else
                    {
                        e = new RtreeEntry(n.m_pIdentifier[cChild],false);
                    }
                    double mind;
                    if(type.equalsIgnoreCase("clustermbr") && n.m_level != 0)
                        mind = irscore;
                    else if(type.equalsIgnoreCase("termmbr") && n.m_level != 0)
                        mind = irscore;
                    else
                        mind = combinedScore(n.m_pMBR[cChild].getMinimumDistance(q.qpoint), irscore);

                    queue.add(new NNEntry(e, mind));

					/*if(e.getIdentifier() == 32483){
						System.out.println("entry:" + e.getIdentifier() + "," + n.m_pMBR[cChild].getLow(0) + " " + n.m_pMBR[cChild].getLow(1) + ":" + n.m_pMBR[cChild].getHigh(0) + " " + n.m_pMBR[cChild].getHigh(1));
						System.out.println(n.m_pMBR[cChild].getMinimumDistance(q.qpoint));
						System.out.println(irscore);
						System.exit(-1);
					}*/

                }
            }
        }

    }

    public double averageMBRsize(InvertedFileWeight invertedFile, DocumentStore cm, String type) throws Exception{
        Node n = readNode(m_rootID);
        double size = averageMBRsizePostOrder(invertedFile, cm, n, type);
        return size;
    }
    private double averageMBRsizePostOrder(InvertedFileWeight invertedFile, DocumentStore cm, Node n, String type) throws Exception{
        if(n.m_level == 0){
            return 0;
        }
        else{
            double size = 0;
            for(int child = 0; child < n.m_children; child++){
                Node nn = readNode(n.m_pIdentifier[child]);
                size += averageMBRsizePostOrder(invertedFile, cm, nn, type);
            }
            return size;
        }
    }

//	public void RtreeAccess(HashSet qw, Region qmbr, Vector objwords){
//		Node n = readNode(m_rootID);
//		AccesspostOrder(n, qw, qmbr, objwords);
//	}
//	void AccesspostOrder(Node n, HashSet qw, Region qmbr, Vector objwords){
//		if(n.m_level == 0){
//			LeafNodeAccessPro.rtreenodecounter++;
//			boolean keywordflag = false;
//			boolean mbrflag = false;
//
//			HashSet nodeword = new HashSet();
//
//			for(int child = 0; child < n.m_children; child++){
//				int id = n.m_pIdentifier[child];
//				HashSet words = (HashSet)objwords.get(id);
//				nodeword.addAll(words);
//			}
//			if(nodeword.containsAll(qw)){
//				LeafNodeAccessPro.rtreekeywordmatch++;
//				keywordflag = true;
//			}
//
//			if(qmbr.getMinimumDistance(n.m_nodeMBR) <= LeafNodeAccessPro.knndistance){
//					LeafNodeAccessPro.rtreembrintersection++;
//					mbrflag = true;
//			}
//
//			if(keywordflag && mbrflag)
//				LeafNodeAccessPro.rtreeboth++;
//		}
//		else{
//			for(int child = 0; child < n.m_children; child++){
//				Node nn = readNode(n.m_pIdentifier[child]);
//				AccesspostOrder(nn, qw, qmbr, objwords);
//			}
//		}
//	}

//	public void postingIO(HashSet qw, Region qmbr, Vector objwords){
//		Node n = readNode(m_rootID);
//		postingIOpostOrder(n, qw, qmbr, objwords);
//	}
//	HashSet postingIOpostOrder(Node n, HashSet qw, Region qmbr, Vector objwords){
//		if(n.m_level == 0){
//
//			HashSet nodeword = new HashSet();
//
//			for(int child = 0; child < n.m_children; child++){
//				int id = n.m_pIdentifier[child];
//				HashSet words = (HashSet)objwords.get(id);
//				nodeword.addAll(words);
//			}
//			return nodeword;
//		}
//		else{
//			HashSet nodewordparent = new HashSet();
//			NonLeafPostingList.ir09pcount = 0;
//			for(int child = 0; child < n.m_children; child++){
//				Node nn = readNode(n.m_pIdentifier[child]);
//				HashSet nodeword = postingIOpostOrder(nn, qw, qmbr, objwords);
//				nodewordparent.addAll(nodeword);
//
//				if(nodeword.containsAll(qw)){
//					NonLeafPostingList.ir11pcount++;
//					NonLeafPostingList.ir09pcount++;
//				}
//			}
//
//			if(qmbr.getMinimumDistance(n.m_nodeMBR) <= NonLeafPostingList.knndistance){
//
//				NonLeafPostingList.ir09IO += (int)Math.ceil(NonLeafPostingList.ir09pcount*12.0/NonLeafPostingList.pagesize);
//			}
//
//			return nodewordparent;
//		}
//	}
}
