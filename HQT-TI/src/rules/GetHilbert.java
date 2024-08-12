package rules;//import java.lang.*;


import com.sun.org.apache.xpath.internal.operations.Gt;

import java.io.*;
import java.util.HashMap;
import java.util.Vector;
import static rules.RegionQueryProcess.*;




public class GetHilbert {
    public static  float global_x1=-180f;
    public static  float global_y1=-90f;
    public static  float global_x2=180f;
    public static  float global_y2=90f;
    public static  int NumObjAdded=0;
    public static  float Granularity=999999999l;
   /* public static  float   global_x1=0;
    public static  float   global_y1=0;
    public static  float   global_x2=10;
    public static  float   global_y2=10;
    public static  int NumObjAdded=0;
    public static  float Granularity=999999999l;*/

    // public static  int state=0;
   // public static Vector<Long> longs;
    public static HashMap<Long, Node> QuadTree;
    public static Vector<Gt> HilbertResults;

    //public static Vector<Integer> IVs = new Vector<>();

    static BufferedReader brl;
    static BufferedReader brt;
    static OutputStreamWriter locr;
    static OutputStreamWriter docr;



    public static  Vector<Node> ConfirmState(int SState, long id1){
        Node n = QuadTree.get(id1);
        if(id1*4+4 > Granularity){
            n.IsLeaf = true;
            System.err.println("nid1: " + id1 + " is considered to be leaf due to the Maxid.");

        }
        //Vector<rules.Node> nodes = new Vector<>();
        switch (SState){
            case 0:
                Vector<Node> nodes0 = new Vector<>();
                Node child_NW0 = new Node(n.b_x1, (n.b_y1 + n.b_y2) / 2, (n.b_x1 + n.b_x2)/2, n.b_y2);
                child_NW0.state = 0;
                nodes0.add(child_NW0);
                Node child_NE0 = new Node((n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2, n.b_x2, n.b_y2);
                child_NE0.state = 0;
                nodes0.add(child_NE0);
                Node child_SW0 = new Node(n.b_x1, n.b_y1, (n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2);
                child_SW0.state = 1;
                nodes0.add(child_SW0);
                Node child_SE0 = new Node((n.b_x1 + n.b_x2) / 2, n.b_y1, n.b_x2, (n.b_y1 + n.b_y2) / 2);
                child_SE0.state = 3;
                nodes0.add(child_SE0);
                return nodes0;



            case 1:
                Vector<Node>nodes1=new Vector<>();
                Node child_NW1 = new Node(n.b_x1, (n.b_y1 + n.b_y2) / 2, (n.b_x1 + n.b_x2)/2, n.b_y2);
                child_NW1.state = 2;
                nodes1.add(child_NW1);
                Node child_NE1 = new Node((n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2, n.b_x2, n.b_y2);
                child_NE1.state = 1;
                nodes1.add(child_NE1);
                Node child_SW1 = new Node(n.b_x1, n.b_y1, (n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2);
                child_SW1.state = 0;
                nodes1.add(child_SW1);
                Node child_SE1 = new Node((n.b_x1 + n.b_x2) / 2, n.b_y1, n.b_x2, (n.b_y1 + n.b_y2) / 2);
                child_SE1.state = 1;
                nodes1.add(child_SE1);
                return nodes1;

            case 2:
                Vector<Node>nodes2=new Vector<>();
                Node child_NW2 = new Node(n.b_x1, (n.b_y1 + n.b_y2) / 2, (n.b_x1 + n.b_x2)/2, n.b_y2);
                child_NW2.state = 1;
                nodes2.add(child_NW2);
                Node child_NE2 = new Node((n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2, n.b_x2, n.b_y2);
                child_NE2.state = 3;
                nodes2.add(child_NE2);
                Node child_SW2 = new Node(n.b_x1, n.b_y1, (n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2);
                child_SW2.state = 2;
                nodes2.add(child_SW2);
                Node child_SE2 = new Node((n.b_x1 + n.b_x2) / 2, n.b_y1, n.b_x2, (n.b_y1 + n.b_y2) / 2);
                child_SE2.state = 2;
                nodes2.add(child_SE2);
                return nodes2;

            case 3:
                Vector<Node>nodes3=new Vector<>();
                Node child_NW3 = new Node(n.b_x1, (n.b_y1 + n.b_y2) / 2, (n.b_x1 + n.b_x2)/2, n.b_y2);
                child_NW3.state = 3;
                nodes3.add(child_NW3);
                Node child_NE3 = new Node((n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2, n.b_x2, n.b_y2);
                child_NE3.state = 2;
                nodes3.add(child_NE3);
                Node child_SW3 = new Node(n.b_x1, n.b_y1, (n.b_x1 + n.b_x2) / 2, (n.b_y1 + n.b_y2) / 2);
                child_SW3.state = 3;
                nodes3.add(child_SW3);
                Node child_SE3 = new Node((n.b_x1 + n.b_x2) / 2, n.b_y1, n.b_x2, (n.b_y1 + n.b_y2) / 2);
                child_SE3.state = 0;
                nodes3.add(child_SE3);
                return nodes3;

        }

        return null;
    }

    public static void AssignObjToChildNodes(long Iid){
        Node n1=QuadTree.get(Iid);

        if (Iid*4+4>Granularity){
            n1.IsLeaf=true;
            System.err.println("Iid: " + Iid + " is considered to be leaf due to the Maxid.");
            return;
        }

        Vector<Node> node = ConfirmState(n1.state, Iid);
        Node child_NW=node.get(0);
        Node child_NE=node.get(1);
        Node child_SW=node.get(2);
        Node child_SE=node.get(3);

        for(int i=0;i<n1.GtList.size();i++){
            Gt obj1 = n1.GtList.get(i);
            if (i==10000){
                System.out.println("YEAH!!!!!!!!!!!!  " + Iid);
                System.out.println("NodeSize: " + n1.GtList.size());
            }
            if (child_NW.ContainObj(obj1)){

                child_NW.GtList.add(obj1);
            }
            else if (child_NE.ContainObj(obj1)){
                child_NE.GtList.add(obj1);
            }
            else if (child_SW.ContainObj(obj1)){
                child_SW.GtList.add(obj1);
            }
            else if (child_SE.ContainObj(obj1)) {
                child_SE.GtList.add(obj1);
            }

            else{
                System.err.println("Error in GetZorder.java !");
                System.err.println("obj.oid: " + obj1.oid);
                System.err.println("obj.olat: " + obj1.olat + "\t obj.olng: " + obj1.olng);
                System.err.println("n1.b_x1: " + n1.b_x1 + "\t n1.b_x2: " + n1.b_x2 + "\t n1.b_y1: " + n1.b_y1 + "\t n1.b_y2: " + n1.b_y2);

            }
        }
        n1.GtList.clear();

        child_NW.SetBorder();
        child_NE.SetBorder();
        child_SW.SetBorder();
        child_SE.SetBorder();

        if (n1.state==0) {
            QuadTree.put(Iid * 4 + 1, child_SW);
            QuadTree.put(Iid * 4 + 2, child_NW);
            QuadTree.put(Iid * 4 + 3, child_NE);
            QuadTree.put(Iid * 4 + 4, child_SE);
        }
        if (n1.state==1) {

            QuadTree.put(Iid * 4 + 1, child_SW);
            QuadTree.put(Iid * 4 + 2, child_SE);
            QuadTree.put(Iid * 4 + 3, child_NE);
            QuadTree.put(Iid * 4 + 4, child_NW);
        }
        if (n1.state==2) {
            QuadTree.put(Iid * 4 + 1, child_NE);
            QuadTree.put(Iid * 4 + 2, child_SE);
            QuadTree.put(Iid * 4 + 3, child_SW);
            QuadTree.put(Iid * 4 + 4, child_NW);
        }
        if (n1.state==3) {
            QuadTree.put(Iid * 4 + 1, child_SW);
            QuadTree.put(Iid * 4 + 2, child_NW);
            QuadTree.put(Iid * 4 + 3, child_NE);
            QuadTree.put(Iid * 4 + 4, child_SE);
        }



        Node node1 = QuadTree.get(Iid * 4 + 1);//1，5
        // if (QuadTree.get(Iid*4+1).GtList.size()>1)
        if (node1.GtList.size()>1){
            AssignObjToChildNodes(Iid*4+1);
        }
        else{
            node1.IsLeaf=true;
        }
        Node node2 = QuadTree.get(Iid * 4 + 2);
        if (node2.GtList.size()>1){
            AssignObjToChildNodes(Iid*4+2);
        }
        else{
            node2.IsLeaf=true;
        }
        Node node3 = QuadTree.get(Iid * 4 + 3);
        if (node3.GtList.size()>1){
            AssignObjToChildNodes(Iid*4+3);
        }
        else{
            node3.IsLeaf=true;
        }
        Node node4 = QuadTree.get(Iid * 4 + 4);
        if (node4.GtList.size()>1){
            AssignObjToChildNodes(Iid*4+4);
        }
        else{
            node4.IsLeaf=true;
        }



    }

    public static void FindOidRange(RegionQuery RQ, long nid){

        double x1 = RQ.region.getLow(0);
        double y1 = RQ.region.getLow(1);
        double x2 = RQ.region.getHigh(0);
        double y2 = RQ.region.getHigh(1);
        Node n=QuadTree.get(nid);
        if (n.ContainsRange(x1, y1, x2,y2) && n.IsLeaf==false){
            //System.out.println(nid);
            //System.out.println(x1+ " " + x2 +" "+" "+y1+" "+y2);
            //System.out.println(n.b_x1+ " " + n.b_x2 +" "+" "+n.b_y1+" "+n.b_y2);
            //System.out.println(n.ContainsRange(x1, y1, x2,y2));
            //System.out.println(nid+" "+rules.RegionQuery.MinOid +" "+rules.RegionQuery.MaxOid );
            RegionQuery.MaxOid=n.MaxOid;
            RegionQuery.MinOid=n.MinOid;
            FindOidRange(RQ,nid*4+1);
            FindOidRange(RQ,nid*4+2);
            FindOidRange(RQ,nid*4+3);
            FindOidRange(RQ,nid*4+4);
            //System.out.println("nid:"+nid);
        }
    }
    public static long FindOidRange1(RegionQuery RQ, long nid){
        long lastNid = nid;
        Node n = QuadTree.get(nid);
        if (n != null && n.ContainsRange(RQ.region.getLow(0), RQ.region.getLow(1), RQ.region.getHigh(0), RQ.region.getHigh(1)) && !n.IsLeaf){
            RegionQuery.MaxOid = n.MaxOid;
            RegionQuery.MinOid = n.MinOid;
            long childNid;
            for (int i = 1; i <= 4; i++) {
                childNid = FindOidRange1(RQ, nid * 4 + i);
                if (childNid != -1) {
                    lastNid = childNid;
                }
            }
        } else {
            lastNid = -1;
        }
        return lastNid;
    }

    public static void BFS2(long nnid) throws Exception{
        Node n = QuadTree.get(nnid);

        if(n.IsLeaf == false){
            BFS2(nnid*4+1);
            BFS2(nnid*4+2);
            BFS2(nnid*4+3);
            BFS2(nnid*4+4);
        }
        else{
            for(int k=0; k < n.GtList.size(); k++){
                Gt gg = n.GtList.get(k);
                /* debug */
                if(nnid == 501014046294l){
                    System.err.println("k: " + k);
                    System.err.println("n.GtList.size(): " + n.GtList.size());
                    System.err.println("YEAHHAHA!!!!!!!!!!!!  " + nnid);
                    System.err.println("GtList.get(0): " + n.GtList.get(0).oid);
                    System.err.println("GtList.get(1): " + n.GtList.get(1).oid);
                }
                locr.write(gg.oid + "," + gg.olat + "," + gg.olng + "\r\n");
                docr.write(String.valueOf(gg.oid));
                for(int j=0; j<gg.Keywords.size(); j++){
                    docr.write("," + gg.Keywords.get(j));
                }
                docr.write("\r\n");
            }
        }
    }

    public static void dfs(long nid, int currentDepth, int maxDepth,RegionQuery RQ) {

        double x1 = RQ.region.getLow(0);
        double y1 = RQ.region.getLow(1);
        double x2 = RQ.region.getHigh(0);
        double y2 = RQ.region.getHigh(1);

        Node Node = QuadTree.get(nid);
        if (Node == null ||currentDepth > maxDepth ||(Node.IsLeaf==true && Node.GtList.size()==0)) {

            return;
        }

        if (Node.Intersect(x1,y1,x2,y2) && (currentDepth == maxDepth || Node.IsLeaf)) {
            IVs.add(Node.MinOid);
            IVs.add(Node.MaxOid);
        }

        if (Node.IsLeaf) {
            return;
        }
        dfs(nid*4+1, currentDepth + 1, maxDepth, RQ);
        dfs(nid*4+2, currentDepth + 1, maxDepth, RQ);
        dfs(nid*4+3, currentDepth + 1, maxDepth, RQ);
        dfs(nid*4+4, currentDepth + 1, maxDepth, RQ);

    }
    public static void FTFO(RegionQuery RQ,Long nid){
        double x1 = RQ.region.getLow(0);
        double y1 = RQ.region.getLow(1);
        double x2 = RQ.region.getHigh(0);
        double y2 = RQ.region.getHigh(1);
        Node Node = QuadTree.get(nid);
        if (Node.IsLeaf==true && Node.GtList.size()>0){
            if (LastID+1==Node.MinOid){
                IVs.remove(IVs.size()-1);
                IVs.add(Node.MaxOid);
                LastID=Node.MaxOid;
            }else {
                IVs.add(Node.MinOid);
                IVs.add(Node.MaxOid);
                LastID=Node.MaxOid;
            }
            return;
        }
        long child=nid *4;long child1=nid *4+1;long child2=nid *4+2;long child3=nid *4+3;long child4=nid *4+4;
        Node n1= QuadTree.get(child + 1);
        Node n2= QuadTree.get(child + 2);
        Node n3= QuadTree.get(child + 3);
        Node n4= QuadTree.get(child + 4);
        boolean result1 = n1.Intersect(x1, y1, x2, y2);
        boolean result2 = n2.Intersect(x1, y1, x2, y2);
        if (result1==true){
            if (result2==false){
                if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                    ONs.add(child1);
                }
                if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                    ONs.add(child4);
                }
            }
            if (result2==true){ //11
                if (n3.Intersect(x1, y1, x2, y2)==true){
                    //1111
                    if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                        ONs.add(child1);
                    }
                    if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                        ONs.add(child2);
                    }
                    if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                        ONs.add(child3);
                    }
                    if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                        ONs.add(child4);
                    }

                } else{
                    //11
                    if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                        ONs.add(child1);
                    }
                    if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                        ONs.add(child2);
                    }

                }
            }
        }else{//01
            if (result2==true){

                if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                    ONs.add(child2);
                }
                if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                    ONs.add(child3);
                }
            }else {//00
                if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                    ONs.add(child3);
                }
                if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                    ONs.add(child4);
                }
            }
        }
    }

    public static void FTFOS(RegionQuery RQ,Long nid){
        double x1 = RQ.region.getLow(0);
        double y1 = RQ.region.getLow(1);
        double x2 = RQ.region.getHigh(0);
        double y2 = RQ.region.getHigh(1);
        Node Node = QuadTree.get(nid);
        if (Node.IsLeaf==true && Node.GtList.size()>0){
            if (LastID+1==Node.MinOid){
                IVs.remove(IVs.size()-1);
                IVs.add(Node.MaxOid);
                LastID=Node.MaxOid;
            }else {
                IVs.add(Node.MinOid);
                IVs.add(Node.MaxOid);
                LastID=Node.MaxOid;
            }
            return;
        }
        long child=nid *4;  long child1=nid *4+1;long child2=nid *4+2;long child3=nid *4+3;long child4=nid *4+4;
        Node n1= QuadTree.get(child + 1);
        Node n2= QuadTree.get(child + 2);
        Node n3= QuadTree.get(child + 3);
        Node n4= QuadTree.get(child + 4);
        boolean result1 = n1.Intersect(x1, y1, x2, y2);
        boolean result3 = n3.Intersect(x1, y1, x2, y2);
        if (result1==true && result3==true){
            if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                ONs.add(child1);
            }
            if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                ONs.add(child2);
            }
            if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                ONs.add(child3);
            }
            if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                ONs.add(child4);
            }
        }
        if (result1==false && result3==false){
            if(n2.Intersect(x1, y1, x2, y2)==true){
                if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                    ONs.add(child2);
                }
            }else {
                if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                    ONs.add(child4);
                }
            }
        }
        if (result1==true && result3==false){
            //1_0_
            if(n2.Intersect(x1, y1, x2, y2)==true){
                //1100
                if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                    ONs.add(child1);
                }
                if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                    ONs.add(child2);
                }
            }else {
                if (n4.Intersect(x1, y1, x2, y2)==true){
                    //1001
                    if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                        ONs.add(child1);
                    }
                    if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                        ONs.add(child4);
                    }
                }else{
                    //1000
                    if (n1.IsLeaf==false || (n1.IsLeaf==true && n1.GtList.size()>0)){
                        ONs.add(child1);
                    }
                }

            }
        }
        if (result1==false && result3==true){
            //0_1_
            if(n2.Intersect(x1, y1, x2, y2)==true){
                //011_
                if (n2.IsLeaf==false || (n2.IsLeaf==true && n2.GtList.size()>0)){
                    ONs.add(child2);
                }
                if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                    ONs.add(child3);
                }
            }else {
                //001_
                if (n4.Intersect(x1, y1, x2, y2)==true){
                    //0011
                    if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                        ONs.add(child3);
                    }
                    if (n4.IsLeaf==false || (n4.IsLeaf==true && n4.GtList.size()>0)){
                        ONs.add(child4);
                    }
                }else {
                    //0010
                    if (n3.IsLeaf==false || (n3.IsLeaf==true && n3.GtList.size()>0)){
                        ONs.add(child3);
                    }
                }

            }

        }

    }

    public static void cutVector(Vector<Integer> vector) {
        int i = 1;
        while (i < vector.size() - 1) {
            if (vector.get(i) + 1 == vector.get(i + 1)) {
                vector.remove(i);
                vector.remove(i);
            } else {
                i += 2;
            }
        }
    }


    public static int processVector(Vector<Integer> vector, RegionQuery RQ) {

        return RQ.DAAT(RegionQueryProcess.db, vector).size();

    }

    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        QuadTree = new HashMap<Long, Node>();
        String location_file = "*****";
        String text_file = "*****";
        String ordered_location_file = "*****";
        String ordered_text_file = "*****";
        HilbertResults = new Vector<Gt>();
        brl = new BufferedReader(new InputStreamReader(new FileInputStream(location_file)));
        brt = new BufferedReader(new InputStreamReader(new FileInputStream(text_file)));
        locr = new OutputStreamWriter(new FileOutputStream(ordered_location_file));
        docr = new OutputStreamWriter(new FileOutputStream(ordered_text_file));
        String str;
        String strt;
        int count=0;
        Node n_max = new Node(global_x1, global_y1, global_x2, global_y2);
        n_max.state=0;
        while((str= brl.readLine())!=null){
            String[] temp=str.split(",");
            int id=Integer.parseInt(temp[0]);
            double x=Double.parseDouble(temp[1]);
            double y=Double.parseDouble(temp[2]);
            Gt obj = new Gt(count++, (float)y, (float)x);
            strt=brt.readLine();
            String[] tempt=strt.split(",");
            for (int k=1;k<tempt.length;k++){
                int term=Integer.parseInt(tempt[k]);
                obj.Keywords.add(term);

            }
            n_max.GtList.add(obj);

        }
        n_max.SetBorder();
        QuadTree.put(0l,n_max);
        AssignObjToChildNodes(0l);
        BFS2(0l);
        locr.flush();locr.close();
        docr.flush();docr.close();

    }


}











