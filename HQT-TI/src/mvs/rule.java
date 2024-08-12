package mvs;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Comparator;
import java.util.Vector;

public class rule {

    public static Vector<Vector<Integer>> vv = new Vector<>();
    public static int rule_number = 2000;
    public static int[] tj = new int[1000005];

    public static void generate_rule() throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader("**************"));
        LineNumberReader reader1 = new LineNumberReader(new FileReader("******************"));
        String line;
        String[] temp;
        int id=0;
        while((line = reader1.readLine())!=null){
            temp = line.split(",");
            for (int i=1;i<temp.length;i++){
                int k = Integer.parseInt(temp[i]);
                tj[k]++;
            }
        }
        while ((line = reader.readLine()) != null){
            if (id==rule_number)
                break;
            temp = line.split(",");
            Vector<Integer> v = new Vector<>();
            for(int i=0;i<temp.length;i++){
                v.add(Integer.parseInt(temp[i]));
            }
            vv.add(v);
            id++;
        }
        //System.out.println(vv.size());
        vv.sort(new Comparator<Vector<Integer>>() {
            @Override
            public int compare(Vector<Integer> o1, Vector<Integer> o2) {
                //System.out.println(o1.size()+" "+o2.size());
                if(o1.get(o1.size()-2)>o2.get(o2.size()-2))
                    return -1;
                if(o1.get(o1.size()-2)<o2.get(o2.size()-2))
                    return 1;
                return 0;
            }
        });
    }

    public static boolean comp(Vector<Integer> v1,Vector<Integer> v2){
        //System.out.println(v1);
        //System.out.println(v2);
        int count = 0;
        for(int i=0;i<v2.size();i++){
            //System.out.println(v2.get(i)+" "+v1.get(count));
            if ( v2.get(i).equals(v1.get(count))){
                //System.out.println(v2.get(i)+" "+v1.get(count));
                count++;
            }
            if(count==v1.size()-2)
                break;
        }
        //System.out.println(count+" "+v1.size());
        if(count==v1.size()-2)
            return true;
        return false;
    }

    public static void main(String[] args){
        Vector<Integer> v1 = new Vector<>();
        v1.add(1);
        v1.add(2);
        v1.add(3);
        v1.add(4);
        v1.add(5);
        v1.add(6);
        Vector<Integer> v2 = new Vector<>();
        v2.add(2);
        v2.add(3);
        v2.add(6);
        System.out.println(comp(v2,v1));
    }
}
