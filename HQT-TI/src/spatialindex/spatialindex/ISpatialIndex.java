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
// License along with this library; if not, write to the Free Software
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

package spatialindex.spatialindex;

import java.util.PriorityQueue;
import java.util.Vector;

import jdbm.btree.BTree;
import query.Group;
import query.Query;
import query.RegionQuery;
import spatialindex.storagemanager.PropertySet;
import storage.BitmapStore;
import storage.DocumentStore;
import documentindex.InvertedBitmap;
import documentindex.InvertedFile;
import documentindex.InvertedFileWeight;

public interface ISpatialIndex
{
    public void flush() throws IllegalStateException;
    public void insertData(final byte[] data, final IShape shape, int id) throws Exception;
    public boolean deleteData(final IShape shape, int id) throws Exception;
    public void containmentQuery(final IShape query, final IVisitor v);
    public void intersectionQuery(final IShape query, final IVisitor v);
    public void pointLocationQuery(final IShape query, final IVisitor v);
    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v, INearestNeighborComparator nnc);
    public void nearestNeighborQuery(int k, final IShape query, final IVisitor v);
    public void queryStrategy(final IQueryStrategy qs);
    public PropertySet getIndexProperties();
    public void addWriteNodeCommand(INodeCommand nc);
    public void addReadNodeCommand(INodeCommand nc);
    public void addDeleteNodeCommand(INodeCommand nc);
    public boolean isIndexValid();
    public IStatistics getStatistics();
    public Vector rangeQuery1(Region region);

    public void IR(DocumentStore ds, InvertedFile invertedFile);
    public void IBR(DocumentStore ds, InvertedBitmap invertedBitmap);
    public void CIBR(BTree tree, DocumentStore ds, InvertedBitmap invertedBitmap);
    public void IR2(DocumentStore ds, BitmapStore bs);

    public void Iterate_IR(InvertedFile invertedFile, Query q, int topk) throws Exception;
    public void Iterate_IR_range(InvertedFile invertedFile, RegionQuery rq) throws Exception;
    public void Iterate_IBR(InvertedBitmap invertedBitmap, Query q, int topk) throws Exception;
    public void Iterate_CIBR(InvertedBitmap invertedBitmap, Query q, int topk) throws Exception;
    public void Iterate_IR2(BitmapStore bs, DocumentStore ds, Query q, int topk) throws Exception;
    public void Iterate_IR2_range(BitmapStore bs, DocumentStore ds, RegionQuery rq) throws Exception;
    public void Iterate_IBR_range(InvertedBitmap invertedBitmap, RegionQuery rq) throws Exception;
    public void Iterate_CIBR_range(InvertedBitmap invertedBitmap, RegionQuery rq) throws Exception;

    public void Group_IBR(InvertedBitmap invertedBitmap, Group group, int topk, PriorityQueue[] result);
    public void Group_IR(InvertedFile invertedFile, Group group, int topk, PriorityQueue[] result);
    public void Group_IR2(BitmapStore bs, DocumentStore ds, Group group, int topk, PriorityQueue[] result) throws Exception;
    public void Group_CIBR(InvertedBitmap invertedBitmap, Group group, int topk, PriorityQueue[] result);

    public int getIO();
    public Vector getLeaf();

    public void enqueueRoot(PriorityQueue queue, int treeid);
    public void enqueueNode(PriorityQueue queue, RtreeEntry entry, Query q);
    public void enqueueNode1(PriorityQueue queue, RtreeEntry entry, Region region);
    public void enqueueNode2(PriorityQueue queue, RtreeEntry entry, Region region);
    public RtreeEntry getroot(int treeid);
    public boolean isleafexist(Region leafregion, RtreeEntry entry);

    //public int RkT(InvertedFileWeight invertedFile, RegionQuery q, int topk, int filtering) throws Exception;
    //public SimpleRangeKnnManager RangekNN(InvertedFileWeight invertedFile, RegionQuery q, int topk, int filtering, int cluster) throws Exception;
} // ISpatialIndex

