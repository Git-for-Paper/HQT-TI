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

package spatialindex.dirtree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Stack;
import java.util.Vector;

import spatialindex.spatialindex.INode;
import spatialindex.spatialindex.IShape;
import spatialindex.spatialindex.Region;
import spatialindex.spatialindex.SpatialIndex;

abstract class Node implements INode
{
    protected DIRTree m_pTree = null;
    // Parent of all nodes.

    protected int m_level = -1;
    // The level of the node in the tree.
    // Leaves are always at level 0.

    protected int m_identifier = -1;
    // The unique ID of this node.

    protected int m_children = 0;
    // The number of children pointed by this node.

    protected int m_capacity = -1;
    // Specifies the node capacity.

    protected Region m_nodeMBR = null;
    // The minimum bounding region enclosing all data contained in the node.

    protected byte[][] m_pData = null;
    // The data stored in the node.

    protected Region[] m_pMBR = null;
    // The corresponding data MBRs.

    protected int[] m_pIdentifier = null;
    // The corresponding data identifiers.

    protected int[] m_pDataLength = null;

    int m_totalDataLength = 0;

    //
    // Abstract methods
    //

    protected abstract Node chooseSubtree(Region mbr, int level, Stack pathBuffer, Vector inv);
    protected abstract Leaf findLeaf(Region mbr, int id, Stack pathBuffer);
    protected abstract Node[] split(byte[] pData, Region mbr, int id) throws Exception;

    //
    // IEntry interface
    //

    public int getIdentifier()
    {
        return m_identifier;
    }

    public IShape getShape()
    {
        return (IShape) m_nodeMBR.clone();
    }

    //
    // INode interface
    //

    public int getChildrenCount()
    {
        return m_children;
    }

    public int getChildIdentifier(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= m_children) throw new IndexOutOfBoundsException("" + index);

        return m_pIdentifier[index];
    }

    public IShape getChildShape(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= m_children) throw new IndexOutOfBoundsException("" + index);

        return new Region(m_pMBR[index]);
    }

    public int getLevel()
    {
        return m_level;
    }

    public boolean isLeaf()
    {
        return (m_level == 0);
    }

    public boolean isIndex()
    {
        return (m_level != 0);
    }

    //
    // Internal
    //

    protected Node(DIRTree pTree, int id, int level, int capacity)
    {
        m_pTree = pTree;
        m_level = level;
        m_identifier = id;
        m_capacity = capacity;
        m_nodeMBR = (Region) pTree.m_infiniteRegion.clone();

        m_pDataLength = new int[m_capacity + 1];
        m_pData = new byte[m_capacity + 1][];
        m_pMBR = new Region[m_capacity + 1];
        m_pIdentifier = new int[m_capacity + 1];
    }

    protected void insertEntry(byte[] pData, Region mbr, int id) throws IllegalStateException
    {
        if (m_children >= m_capacity) throw new IllegalStateException("m_children >= m_nodeCapacity");

        m_pDataLength[m_children] = (pData != null) ? pData.length : 0;
        m_pData[m_children] = pData;
        m_pMBR[m_children] = mbr;
        m_pIdentifier[m_children] = id;

        m_totalDataLength += m_pDataLength[m_children];
        m_children++;

        Region.combinedRegion(m_nodeMBR, mbr);


    }

    protected void deleteEntry(int index) throws IndexOutOfBoundsException
    {
        if (index < 0 || index >= m_children) throw new IndexOutOfBoundsException("" + index);

        boolean touches = m_nodeMBR.touches(m_pMBR[index]);

        m_totalDataLength -= m_pDataLength[index];
        m_pData[index] = null;

        if (m_children > 1 && index != m_children - 1)
        {
            m_pDataLength[index] = m_pDataLength[m_children - 1];
            m_pData[index] = m_pData[m_children - 1];
            m_pData[m_children - 1] = null;
            m_pMBR[index] = m_pMBR[m_children - 1];
            m_pMBR[m_children - 1] = null;
            m_pIdentifier[index] = m_pIdentifier[m_children - 1];
        }

        m_children--;

        if (m_children == 0)
        {
            m_nodeMBR = (Region) m_pTree.m_infiniteRegion.clone();
        }
        else if (touches)
        {
            for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
            {
                m_nodeMBR.m_pLow[cDim] = Double.POSITIVE_INFINITY;
                m_nodeMBR.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < m_children; cChild++)
                {
                    m_nodeMBR.m_pLow[cDim] = Math.min(m_nodeMBR.m_pLow[cDim], m_pMBR[cChild].m_pLow[cDim]);
                    m_nodeMBR.m_pHigh[cDim] = Math.max(m_nodeMBR.m_pHigh[cDim], m_pMBR[cChild].m_pHigh[cDim]);
                }
            }
        }
    }

    protected boolean insertData(byte[] pData, Region mbr, int id, Stack pathBuffer, boolean[] overflowTable) throws Exception
    {
        if (m_children < m_capacity)
        {
            boolean adjusted = false;
            //boolean b = m_nodeMBR.contains(mbr);
            boolean b = false;

            insertEntry(pData, mbr, id);
            m_pTree.writeNode(this);
            //////////////////////////////////////////////////////
            HashSet centerv;
            Vector obj;
            HashSet entry;
            try{
                centerv = (HashSet)DIRTree.docbtree.find(this.m_identifier);
                if(this.isLeaf()){
                    obj = DIRTree.objstore.read(id, false);
                    if(centerv == null){
                        centerv = new HashSet();
                        for(int i = 0; i < obj.size(); i++){
                            int w = (Integer)obj.get(i);
                            centerv.add(w);
                        }
                        DIRTree.docbtree.insert(this.m_identifier, centerv, true);
                    }
                    else{
                        union(obj, centerv);
                        DIRTree.docbtree.insert(this.m_identifier, centerv, true);
                    }
                }
                else{
                    entry = (HashSet)DIRTree.docbtree.find(id);
                    if(centerv == null){
                        DIRTree.docbtree.insert(this.m_identifier, entry, true);
                    }
                    else{
                        centerv.addAll(entry);
                        DIRTree.docbtree.insert(this.m_identifier, centerv, true);
                    }
                }

            }
            catch(Exception e){
                e.printStackTrace();
            }
            ///////////////////////////////////////////////////////////////
            if (! b && ! pathBuffer.empty())
            {
                int cParent = ((Integer) pathBuffer.pop()).intValue();
                Index p = (Index) m_pTree.readNode(cParent);
                p.adjustTree(this, pathBuffer);
                adjusted = true;
            }

            return adjusted;
        }
        else if (m_pTree.m_treeVariant == SpatialIndex.RtreeVariantRstar && ! pathBuffer.empty() && overflowTable[m_level] == false)
        {
            overflowTable[m_level] = true;

            ArrayList vReinsert = new ArrayList(), vKeep = new ArrayList();
            reinsertData(pData, mbr, id, vReinsert, vKeep);

            int lReinsert = vReinsert.size();
            int lKeep = vKeep.size();

            byte[][] reinsertdata = new byte[lReinsert][];
            Region[] reinsertmbr = new Region[lReinsert];
            int[] reinsertid = new int[lReinsert];
            int[] reinsertlen = new int[lReinsert];
            byte[][] keepdata = new byte[m_capacity + 1][];
            Region[] keepmbr = new Region[m_capacity + 1];
            int[] keepid = new int[m_capacity + 1];
            int[] keeplen = new int[m_capacity + 1];

            int cIndex;

            for (cIndex = 0; cIndex < lReinsert; cIndex++)
            {
                int i = ((Integer) vReinsert.get(cIndex)).intValue();
                reinsertlen[cIndex] = m_pDataLength[i];
                reinsertdata[cIndex] = m_pData[i];
                reinsertmbr[cIndex] = m_pMBR[i];
                reinsertid[cIndex] = m_pIdentifier[i];
            }

            for (cIndex = 0; cIndex < lKeep; cIndex++)
            {
                int i = ((Integer) vKeep.get(cIndex)).intValue();
                keeplen[cIndex] = m_pDataLength[i];
                keepdata[cIndex] = m_pData[i];
                keepmbr[cIndex] = m_pMBR[i];
                keepid[cIndex] = m_pIdentifier[i];
            }

            m_pDataLength = keeplen;
            m_pData = keepdata;
            m_pMBR = keepmbr;
            m_pIdentifier = keepid;
            m_children = lKeep;
            m_totalDataLength = 0;
            for (int cChild = 0; cChild < m_children; cChild++) m_totalDataLength += m_pDataLength[cChild];

            for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
            {
                m_nodeMBR.m_pLow[cDim] = Double.POSITIVE_INFINITY;
                m_nodeMBR.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;

                for (int cChild = 0; cChild < m_children; cChild++)
                {
                    m_nodeMBR.m_pLow[cDim] = Math.min(m_nodeMBR.m_pLow[cDim], m_pMBR[cChild].m_pLow[cDim]);
                    m_nodeMBR.m_pHigh[cDim] = Math.max(m_nodeMBR.m_pHigh[cDim], m_pMBR[cChild].m_pHigh[cDim]);
                }
            }

            m_pTree.writeNode(this);

            // Divertion from R*-Tree algorithm here. First adjust
            // the path to the root, then start reinserts, to avoid complicated handling
            // of changes to the same node from multiple insertions.
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) m_pTree.readNode(cParent);
            p.adjustTree(this, pathBuffer);

            for (cIndex = 0; cIndex < lReinsert; cIndex++)
            {
                m_pTree.insertData_impl(reinsertdata[cIndex],
                        reinsertmbr[cIndex],
                        reinsertid[cIndex],
                        m_level, overflowTable);
            }

            return true;
        }
        else
        {
            Node[] nodes = split(pData, mbr, id);
            Node n = nodes[0];
            Node nn = nodes[1];

            if (pathBuffer.empty())
            {
                n.m_identifier = -1;
                nn.m_identifier = -1;
                m_pTree.writeNode(n);
                m_pTree.writeNode(nn);
                /////////////////////////////////////////
                HashSet vn = new HashSet();
                HashSet vnn = new HashSet();
                try{

                    for(int i = 0; i < n.m_children; i++){
                        if(n.isLeaf()){
                            Vector v = DIRTree.objstore.read(n.m_pIdentifier[i], false);
                            union(v, vn);
                        }
                        else{
                            HashSet v = (HashSet)DIRTree.docbtree.find(n.m_pIdentifier[i]);
                            vn.addAll(v);
                        }
                    }
                    DIRTree.docbtree.insert(n.m_identifier, vn, true);

                    for(int i = 0; i < nn.m_children; i++){
                        if(nn.isLeaf()){
                            Vector v = DIRTree.objstore.read(nn.m_pIdentifier[i], false);
                            union(v, vnn);
                        }
                        else{
                            HashSet v = (HashSet)DIRTree.docbtree.find(nn.m_pIdentifier[i]);
                            vnn.addAll(v);
                        }
                    }
                    DIRTree.docbtree.insert(nn.m_identifier, vnn, true);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                //////////////////////////////////////////
                Index r = new Index(m_pTree, m_pTree.m_rootID, m_level + 1);

                r.insertEntry(null, (Region) n.m_nodeMBR.clone(), n.m_identifier);
                r.insertEntry(null, (Region) nn.m_nodeMBR.clone(), nn.m_identifier);

                m_pTree.writeNode(r);

                /////////////////////////
                HashSet vr = new HashSet();
                vr.addAll(vn);
                vr.addAll(vnn);

                try{
                    DIRTree.docbtree.insert(r.m_identifier, vr, true);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                /////////////////////////
                m_pTree.m_stats.m_nodesInLevel.set(m_level, new Integer(2));
                m_pTree.m_stats.m_nodesInLevel.add(new Integer(1));
                m_pTree.m_stats.m_treeHeight = m_level + 2;
            }
            else
            {
                n.m_identifier = m_identifier;
                nn.m_identifier = -1;

                m_pTree.writeNode(n);
                m_pTree.writeNode(nn);

                /////////////////////////////////////////
                try{
                    HashSet vn = new HashSet();
                    for(int i = 0; i < n.m_children; i++){
                        if(n.isLeaf()){
                            Vector v = DIRTree.objstore.read(n.m_pIdentifier[i], false);
                            union(v, vn);
                        }
                        else{
                            HashSet v = (HashSet)DIRTree.docbtree.find(n.m_pIdentifier[i]);
                            vn.addAll(v);
                        }
                    }
                    DIRTree.docbtree.insert(n.m_identifier, vn, true);
                    HashSet vnn = new HashSet();
                    for(int i = 0; i < nn.m_children; i++){
                        if(nn.isLeaf()){
                            Vector v = DIRTree.objstore.read(nn.m_pIdentifier[i], false);
                            union(v, vnn);
                        }
                        else{
                            HashSet v = (HashSet)DIRTree.docbtree.find(nn.m_pIdentifier[i]);
                            vnn.addAll(v);
                        }
                    }
                    DIRTree.docbtree.insert(nn.m_identifier, vnn, true);
                }
                catch(Exception e){
                    e.printStackTrace();
                }
                //////////////////////////////////////////

                int cParent = ((Integer) pathBuffer.pop()).intValue();
                Index p = (Index) m_pTree.readNode(cParent);
                p.adjustTree(n, nn, pathBuffer, overflowTable);
            }

            return true;
        }
    }

    protected void reinsertData(byte[] pData, Region mbr, int id, ArrayList reinsert, ArrayList keep)
    {
        ReinsertEntry[] v = new ReinsertEntry[m_capacity + 1];

        m_pDataLength[m_children] = (pData != null) ? pData.length : 0;
        m_pData[m_children] = pData;
        m_pMBR[m_children] = mbr;
        m_pIdentifier[m_children] = id;

        double[] nc = m_nodeMBR.getCenter();

        for (int cChild = 0; cChild < m_capacity + 1; cChild++)
        {
            ReinsertEntry e = new ReinsertEntry(cChild, 0.0f);

            double[] c = m_pMBR[cChild].getCenter();

            // calculate relative distance of every entry from the node MBR (ignore square root.)
            for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
            {
                double d = nc[cDim] - c[cDim];
                e.m_dist += d * d;
            }

            v[cChild] = e;
        }

        // sort by increasing order of distances.
        Arrays.sort(v, new ReinsertEntryComparator());

        int cReinsert = (int) Math.floor((m_capacity + 1) * m_pTree.m_reinsertFactor);
        int cCount;

        for (cCount = 0; cCount < cReinsert; cCount++)
        {
            reinsert.add(new Integer(v[cCount].m_id));
        }

        for (cCount = cReinsert; cCount < m_capacity + 1; cCount++)
        {
            keep.add(new Integer(v[cCount].m_id));
        }
    }

    protected void rtreeSplit(byte[] pData, Region mbr, int id, ArrayList group1, ArrayList group2) throws Exception
    {
        int cChild;
        int minimumLoad = (int) Math.floor(m_capacity * m_pTree.m_fillFactor);

        // use this mask array for marking visited entries.
        boolean[] mask = new boolean[m_capacity + 1];
        for (cChild = 0; cChild < m_capacity + 1; cChild++) mask[cChild] = false;

        // insert new data in the node for easier manipulation. Data arrays are always
        // by one larger than node capacity.
        m_pDataLength[m_capacity] = (pData != null) ? pData.length : 0;
        m_pData[m_capacity] = pData;
        m_pMBR[m_capacity] = mbr;
        m_pIdentifier[m_capacity] = id;

        // initialize each group with the seed entries.
        int[] seeds = pickSeeds();
        //System.out.println(seeds[0]);

        group1.add(new Integer(seeds[0]));
        group2.add(new Integer(seeds[1]));

        mask[seeds[0]] = true;
        mask[seeds[1]] = true;

        // find MBR of each group.
        Region mbr1 = (Region) m_pMBR[seeds[0]].clone();
        Region mbr2 = (Region) m_pMBR[seeds[1]].clone();

        System.out.println(mbr1+" "+mbr2);
        Vector v1 = (Vector)DIRTree.objstore.read(m_pIdentifier[seeds[0]], false);
        Vector v2 = (Vector)DIRTree.objstore.read(m_pIdentifier[seeds[1]], false);
        HashSet centerv1 = new HashSet();
        HashSet centerv2 = new HashSet();
        System.out.println(v1.size());
        for(int i = 0; i < v1.size(); i++){
            int w = (Integer)v1.get(i);
            centerv1.add(w);
        }
        for(int i = 0; i < v2.size(); i++){
            int w = (Integer)v2.get(i);
            centerv2.add(w);
        }

        // count how many entries are left unchecked (exclude the seeds here.)
        int cRemaining = m_capacity + 1 - 2;

        while (cRemaining > 0)
        {
            if (minimumLoad - group1.size() == cRemaining)
            {
                // all remaining entries must be assigned to group1 to comply with minimun load requirement.
                for (cChild = 0; cChild < m_capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        group1.add(new Integer(cChild));
                        mask[cChild] = true;
                        cRemaining--;
                    }
                }
            }
            else if (minimumLoad - group2.size() == cRemaining)
            {
                // all remaining entries must be assigned to group2 to comply with minimun load requirement.
                for (cChild = 0; cChild < m_capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        group2.add(new Integer(cChild));
                        mask[cChild] = true;
                        cRemaining--;
                    }
                }
            }
            else
            {
                // For all remaining entries compute the difference of the cost of grouping an
                // entry in either group. When done, choose the entry that yielded the maximum
                // difference. In case of linear split, select any entry (e.g. the first one.)
                int sel = -1;
                double md1 = 0.0f, md2 = 0.0f;
                double m = Double.NEGATIVE_INFINITY;
                double d1 = 0, d2 = 0, d;
                double a1 = mbr1.getArea();
                double a2 = mbr2.getArea();
                double cd1, cd2;

                for (cChild = 0; cChild < m_capacity + 1; cChild++)
                {
                    if (mask[cChild] == false)
                    {
                        Vector inv = (Vector)DIRTree.objstore.read(m_pIdentifier[cChild], false);

                        Region a = mbr1.combinedRegion(m_pMBR[cChild]);
                        d1 = a.getArea() - a1;
                        cd1 = d1*DIRTree.alpha_dist + (1-DIRTree.alpha_dist)*HammingDist(inv, centerv1)/centerv1.size();
                        Region b = mbr2.combinedRegion(m_pMBR[cChild]);
                        d2 = b.getArea() - a2;
                        cd2 = d2*DIRTree.alpha_dist + (1-DIRTree.alpha_dist)*HammingDist(inv, centerv2)/centerv2.size();
                        //d = Math.abs(d1 - d2);
                        d = Math.abs(cd1 - cd2);

                        //System.out.println(cd1+" "+HammingDist(inv, centerv1)/centerv1.size());
                        if (d > m)
                        {
                            m = d;
                            //md1 = d1; md2 = d2;
                            md1 = cd1; md2 = cd2;
                            sel = cChild;
                            if (m_pTree.m_treeVariant== SpatialIndex.RtreeVariantLinear || m_pTree.m_treeVariant == SpatialIndex.RtreeVariantRstar) break;
                        }
                    }
                }

                // determine the group where we should add the new entry.
                int group = -1;

                if (md1 < md2)
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (md2 < md1)
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                ////////////////////////////////////
                else if (d1 < d2)
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (d2 < d1)
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                ////////////////////////////////////
                else if (a1 < a2)
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (a2 < a1)
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                else if (group1.size() < group2.size())
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                else if (group2.size() < group1.size())
                {
                    group2.add(new Integer(sel));
                    group = 2;
                }
                else
                {
                    group1.add(new Integer(sel));
                    group = 1;
                }
                //System.out.println(sel);
                mask[sel] = true;
                cRemaining--;
                if (group == 1)
                {
                    Region.combinedRegion(mbr1, m_pMBR[sel]);
                }
                else
                {
                    Region.combinedRegion(mbr2, m_pMBR[sel]);
                }
            }
        }
    }

    protected void rstarSplit(byte[] pData, Region mbr, int id, ArrayList group1, ArrayList group2)
    {
        RstarSplitEntry[] dataLow = new RstarSplitEntry[m_capacity + 1];;
        RstarSplitEntry[] dataHigh = new RstarSplitEntry[m_capacity + 1];;

        m_pDataLength[m_children] = (pData != null) ? pData.length : 0;
        m_pData[m_capacity] = pData;
        m_pMBR[m_capacity] = mbr;
        m_pIdentifier[m_capacity] = id;

        int nodeSPF = (int) (Math.floor((m_capacity + 1) * m_pTree.m_splitDistributionFactor));
        int splitDistribution = (m_capacity + 1) - (2 * nodeSPF) + 2;

        int cChild, cDim, cIndex;

        for (cChild = 0; cChild < m_capacity + 1; cChild++)
        {
            RstarSplitEntry e = new RstarSplitEntry(m_pMBR[cChild], cChild, 0);

            dataLow[cChild] = e;
            dataHigh[cChild] = e;
        }

        double minimumMargin = Double.POSITIVE_INFINITY;
        int splitAxis = -1;
        int sortOrder = -1;

        // chooseSplitAxis.
        for (cDim = 0; cDim < m_pTree.m_dimension; cDim++)
        {
            Arrays.sort(dataLow, new RstarSplitEntryComparatorLow());
            Arrays.sort(dataHigh, new RstarSplitEntryComparatorHigh());

            // calculate sum of margins and overlap for all distributions.
            double marginl = 0.0;
            double marginh = 0.0;

            for (cChild = 1; cChild <= splitDistribution; cChild++)
            {
                int l = nodeSPF - 1 + cChild;

                Region[] tl1 = new Region[l];
                Region[] th1 = new Region[l];

                for (cIndex = 0; cIndex < l; cIndex++)
                {
                    tl1[cIndex] = dataLow[cIndex].m_pRegion;
                    th1[cIndex] = dataHigh[cIndex].m_pRegion;
                }

                Region bbl1 = Region.combinedRegion(tl1);
                Region bbh1 = Region.combinedRegion(th1);

                Region[] tl2 = new Region[m_capacity + 1 - l];
                Region[] th2 = new Region[m_capacity + 1 - l];

                int tmpIndex = 0;
                for (cIndex = l; cIndex < m_capacity + 1; cIndex++)
                {
                    tl2[tmpIndex] = dataLow[cIndex].m_pRegion;
                    th2[tmpIndex] = dataHigh[cIndex].m_pRegion;
                    tmpIndex++;
                }

                Region bbl2 = Region.combinedRegion(tl2);
                Region bbh2 = Region.combinedRegion(th2);

                marginl += bbl1.getMargin() + bbl2.getMargin();
                marginh += bbh1.getMargin() + bbh2.getMargin();
            } // for (cChild)

            double margin = Math.min(marginl, marginh);

            // keep minimum margin as split axis.
            if (margin < minimumMargin)
            {
                minimumMargin = margin;
                splitAxis = cDim;
                sortOrder = (marginl < marginh) ? 0 : 1;
            }

            // increase the dimension according to which the data entries should be sorted.
            for (cChild = 0; cChild < m_capacity + 1; cChild++)
            {
                dataLow[cChild].m_sortDim = cDim + 1;
            }
        } // for (cDim)

        for (cChild = 0; cChild < m_capacity + 1; cChild++)
        {
            dataLow[cChild].m_sortDim = splitAxis;
        }

        if (sortOrder == 0)
            Arrays.sort(dataLow, new RstarSplitEntryComparatorLow());
        else
            Arrays.sort(dataLow, new RstarSplitEntryComparatorHigh());

        double ma = Double.POSITIVE_INFINITY;
        double mo = Double.POSITIVE_INFINITY;
        int splitPoint = -1;

        for (cChild = 1; cChild <= splitDistribution; cChild++)
        {
            int l = nodeSPF - 1 + cChild;

            Region[] t1 = new Region[l];

            for (cIndex = 0; cIndex < l; cIndex++)
            {
                t1[cIndex] = dataLow[cIndex].m_pRegion;
            }

            Region bb1 = Region.combinedRegion(t1);

            Region[] t2 = new Region[m_capacity + 1 - l];

            int tmpIndex = 0;
            for (cIndex = l; cIndex < m_capacity + 1; cIndex++)
            {
                t2[tmpIndex] = dataLow[cIndex].m_pRegion;
                tmpIndex++;
            }

            Region bb2 = Region.combinedRegion(t2);

            double o = bb1.getIntersectingArea(bb2);

            if (o < mo)
            {
                splitPoint = cChild;
                mo = o;
                ma = bb1.getArea() + bb2.getArea();
            }
            else if (o == mo)
            {
                double a = bb1.getArea() + bb2.getArea();

                if (a < ma)
                {
                    splitPoint = cChild;
                    ma = a;
                }
            }
        } // for (cChild)

        int l1 = nodeSPF - 1 + splitPoint;

        for (cIndex = 0; cIndex < l1; cIndex++)
        {
            group1.add(new Integer(dataLow[cIndex].m_id));
        }

        for (cIndex = l1; cIndex <= m_capacity; cIndex++)
        {
            group2.add(new Integer(dataLow[cIndex].m_id));
        }
    }

    protected int[] pickSeeds()
    {
        double separation = Double.NEGATIVE_INFINITY;
        double inefficiency = Double.NEGATIVE_INFINITY;
        int cDim, cChild, cIndex, i1 = 0, i2 = 0;

        System.out.println(m_pTree.m_treeVariant);
        switch (m_pTree.m_treeVariant)
        {
            case SpatialIndex.RtreeVariantLinear:
            case SpatialIndex.RtreeVariantRstar:
                for (cDim = 0; cDim < m_pTree.m_dimension; cDim++)
                {
                    double leastLower = m_pMBR[0].m_pLow[cDim];
                    double greatestUpper = m_pMBR[0].m_pHigh[cDim];
                    int greatestLower = 0;
                    int leastUpper = 0;
                    double width;

                    for (cChild = 1; cChild < m_capacity + 1; cChild++)
                    {
                        if (m_pMBR[cChild].m_pLow[cDim] > m_pMBR[greatestLower].m_pLow[cDim]) greatestLower = cChild;
                        if (m_pMBR[cChild].m_pHigh[cDim] < m_pMBR[leastUpper].m_pHigh[cDim]) leastUpper = cChild;

                        leastLower = Math.min(m_pMBR[cChild].m_pLow[cDim], leastLower);
                        greatestUpper = Math.max(m_pMBR[cChild].m_pHigh[cDim], greatestUpper);
                    }

                    width = greatestUpper - leastLower;
                    if (width <= 0) width = 1;

                    double f = (m_pMBR[greatestLower].m_pLow[cDim] - m_pMBR[leastUpper].m_pHigh[cDim]) / width;

                    if (f > separation)
                    {
                        i1 = leastUpper;
                        i2 = greatestLower;
                        separation = f;
                    }
                }  // for (cDim)

                if (i1 == i2)
                {
                    i2 = (i2 != m_capacity) ? i2 + 1 : i2 - 1;
                }

                break;
            case SpatialIndex.RtreeVariantQuadratic:
                // for each pair of Regions (account for overflow Region too!)
                for (cChild = 0; cChild < m_capacity; cChild++)
                {
                    double a = m_pMBR[cChild].getArea();

                    for (cIndex = cChild + 1; cIndex < m_capacity + 1; cIndex++)
                    {
                        // get the combined MBR of those two entries.
                        Region r = m_pMBR[cChild].combinedRegion(m_pMBR[cIndex]);

                        // find the inefficiency of grouping these entries together.
                        double d = r.getArea() - a - m_pMBR[cIndex].getArea();

                        if (d > inefficiency)
                        {
                            inefficiency = d;
                            i1 = cChild;
                            i2 = cIndex;
                        }
                    }  // for (cIndex)
                } // for (cChild)

                break;
            default:
                throw new IllegalStateException("Unknown RTree variant.");
        }

        int[] ret = new int[2];
        ret[0] = i1;
        ret[1] = i2;
        return ret;
    }

    protected void condenseTree(Stack toReinsert, Stack pathBuffer)
    {
        int minimumLoad = (int) (Math.floor(m_capacity * m_pTree.m_fillFactor));

        if (pathBuffer.empty())
        {
            // eliminate root if it has only one child.
            if (m_level != 0 && m_children == 1)
            {
                Node n = m_pTree.readNode(m_pIdentifier[0]);
                m_pTree.deleteNode(n);
                n.m_identifier = m_pTree.m_rootID;
                m_pTree.writeNode(n);

                m_pTree.m_stats.m_nodesInLevel.remove(m_pTree.m_stats.m_nodesInLevel.size() - 1);
                m_pTree.m_stats.m_treeHeight -= 1;
                // HACK: pending deleteNode for deleted child will decrease nodesInLevel, later on.
                m_pTree.m_stats.m_nodesInLevel.set(m_pTree.m_stats.m_treeHeight - 1, new Integer(2));
            }
        }
        else
        {
            int cParent = ((Integer) pathBuffer.pop()).intValue();
            Index p = (Index) m_pTree.readNode(cParent);

            // find the entry in the parent, that points to this node.
            int child;

            for (child = 0; child != p.m_children; child++)
            {
                if (p.m_pIdentifier[child] == m_identifier) break;
            }

            if (m_children < minimumLoad)
            {
                // used space less than the minimum
                // 1. eliminate node entry from the parent. deleteEntry will fix the parent's MBR.
                p.deleteEntry(child);
                // 2. add this node to the stack in order to reinsert its entries.
                toReinsert.push(this);
            }
            else
            {
                // adjust the entry in 'p' to contain the new bounding region of this node.
                p.m_pMBR[child] = (Region) m_nodeMBR.clone();

                // global recalculation necessary since the MBR can only shrink in size,
                // due to data removal.
                for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
                {
                    p.m_nodeMBR.m_pLow[cDim] = Double.POSITIVE_INFINITY;
                    p.m_nodeMBR.m_pHigh[cDim] = Double.NEGATIVE_INFINITY;

                    for (int cChild = 0; cChild < p.m_children; cChild++)
                    {
                        p.m_nodeMBR.m_pLow[cDim] = Math.min(p.m_nodeMBR.m_pLow[cDim], p.m_pMBR[cChild].m_pLow[cDim]);
                        p.m_nodeMBR.m_pHigh[cDim] = Math.max(p.m_nodeMBR.m_pHigh[cDim], p.m_pMBR[cChild].m_pHigh[cDim]);
                    }
                }
            }

            // write parent node back to storage.
            m_pTree.writeNode(p);

            p.condenseTree(toReinsert, pathBuffer);
        }
    }

    protected void load(byte[] data) throws IOException
    {
        m_nodeMBR = (Region) m_pTree.m_infiniteRegion.clone();

        DataInputStream ds = new DataInputStream(new ByteArrayInputStream(data));

        // skip the node type information, it is not needed.
        ds.readInt();

        m_level = ds.readInt();
        m_children = ds.readInt();

        for (int cChild = 0; cChild < m_children; cChild++)
        {
            m_pMBR[cChild] = new Region();
            m_pMBR[cChild].m_pLow = new double[m_pTree.m_dimension];
            m_pMBR[cChild].m_pHigh = new double[m_pTree.m_dimension];

            for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
            {
                m_pMBR[cChild].m_pLow[cDim] = ds.readDouble();
                m_pMBR[cChild].m_pHigh[cDim] = ds.readDouble();
            }

            m_pIdentifier[cChild] = ds.readInt();

            m_pDataLength[cChild] = ds.readInt();
            if (m_pDataLength[cChild] > 0)
            {
                m_totalDataLength += m_pDataLength[cChild];
                m_pData[cChild] = new byte[m_pDataLength[cChild]];
                ds.read(m_pData[cChild]);
            }
            else
            {
                m_pData[cChild] = null;
            }

            Region.combinedRegion(m_nodeMBR, m_pMBR[cChild]);
        }
    }

    protected byte[] store() throws IOException
    {
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(bs);

        int type;
        if (m_level == 0) type = SpatialIndex.PersistentLeaf;
        else type = SpatialIndex.PersistentIndex;
        ds.writeInt(type);

        ds.writeInt(m_level);
        ds.writeInt(m_children);

        for (int cChild = 0; cChild < m_children; cChild++)
        {
            for (int cDim = 0; cDim < m_pTree.m_dimension; cDim++)
            {
                ds.writeDouble(m_pMBR[cChild].m_pLow[cDim]);
                ds.writeDouble(m_pMBR[cChild].m_pHigh[cDim]);
            }

            ds.writeInt(m_pIdentifier[cChild]);

            ds.writeInt(m_pDataLength[cChild]);
            if (m_pDataLength[cChild] > 0) ds.write(m_pData[cChild]);
        }

        ds.flush();
        return bs.toByteArray();
    }

    class ReinsertEntry
    {
        int m_id;
        double m_dist;
        public ReinsertEntry(int id, double dist) { m_id = id; m_dist = dist; }
    } // ReinsertEntry

    class ReinsertEntryComparator implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            if (((ReinsertEntry) o1).m_dist < ((ReinsertEntry) o2).m_dist) return -1;
            if (((ReinsertEntry) o1).m_dist > ((ReinsertEntry) o2).m_dist) return 1;
            return 0;
        }
    } // ReinsertEntryComparator

    class RstarSplitEntry
    {
        Region m_pRegion;
        int m_id;
        int m_sortDim;

        RstarSplitEntry(Region r, int id, int dimension) { m_pRegion = r; m_id = id; m_sortDim = dimension; }
    }

    class RstarSplitEntryComparatorLow implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            RstarSplitEntry e1 = (RstarSplitEntry) o1;
            RstarSplitEntry e2 = (RstarSplitEntry) o2;

            if (e1.m_pRegion.m_pLow[e1.m_sortDim] < e2.m_pRegion.m_pLow[e2.m_sortDim]) return -1;
            if (e1.m_pRegion.m_pLow[e1.m_sortDim] > e2.m_pRegion.m_pLow[e2.m_sortDim]) return 1;
            return 0;
        }
    }

    class RstarSplitEntryComparatorHigh implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            RstarSplitEntry e1 = (RstarSplitEntry) o1;
            RstarSplitEntry e2 = (RstarSplitEntry) o2;

            if (e1.m_pRegion.m_pHigh[e1.m_sortDim] < e2.m_pRegion.m_pHigh[e2.m_sortDim]) return -1;
            if (e1.m_pRegion.m_pHigh[e1.m_sortDim] > e2.m_pRegion.m_pHigh[e2.m_sortDim]) return 1;
            return 0;
        }
    }

    protected int HammingDist(Vector v1, HashSet centerv){
        HashSet h1 = new HashSet();
        for(int i = 0; i < v1.size(); i++){
            int w = (Integer)v1.get(i);
            h1.add(w);
        }

        HashSet intersection = (HashSet)h1.clone();
        HashSet union = (HashSet)h1.clone();
        intersection.retainAll(centerv);
        union.addAll(centerv);
        int dist = union.size() - intersection.size();

        return dist;
    }

    protected void union(Vector v1, HashSet centerv){
        HashSet h1 = new HashSet();
        for(int i = 0; i < v1.size(); i++){
            int w = (Integer)v1.get(i);
            h1.add(w);
        }
        centerv.addAll(h1);

    }
}
