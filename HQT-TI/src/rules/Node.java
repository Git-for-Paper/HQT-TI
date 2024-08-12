package rules;//import com.sun.org.apache.xpath.internal.operations.rules.Gt;


import java.util.Vector;

public class Node {
        public double b_x1;		// border x1
        public double b_y1;		// border y1
        public double b_x2;		// border x2
        public double b_y2;		// border y2
        public boolean IsLeaf;  //leaf
        public int MaxOid;
        public int MinOid;
        public int state;
        public Vector<Gt> GtList;

    public Node(double x1, double y1, double x2, double y2){
        b_x1 = x1;
        b_x2 = x2;
        b_y1 = y1;
        b_y2 = y2;
        //	MaxOid = 0;
        //	MinOid = Integer.MAX_VALUE;
        IsLeaf = false;
        GtList = new Vector<Gt>();
    }
   public void  SetBorder(){
       int maxid=0;
       int minid=Integer.MAX_VALUE;
       for (int i=0;i<GtList.size();i++){
           int id = GtList.get(i).oid;
           if (id>maxid) {
               maxid = id;
           }
           if (id<minid) {
               minid = id;
           }

       }
       MaxOid=maxid;
       MinOid=minid;

   }
   public boolean ContainObj(Gt obj){
       if (obj.olat<=b_y2 && obj.olat>=b_y1 && obj.olng<=b_x2 && obj.olng>=b_x1){
           return true;
       }
       else {
           return  false;
       }

   }


   public boolean ContainsRange(double x1, double y1, double x2, double y2){
       if(y2 <= b_y2 && y1 >= b_y1 && x2 <= b_x2 && x1 >= b_x1){
           return true;
       }
       else{
           return false;
       }
   }


    public boolean Intersect(double x1, double y1, double x2, double y2){
        if (x2 < b_x1 || x1 > b_x2 || y2 < b_y1 || y1 > b_y2) {
            return false;
        } else {
            return true;
        }
    }






}
