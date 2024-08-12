package mvs;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;


public class rule1 {
    public static HashMap<Set,Integer> mp = new HashMap<>();
    public static int[] tj = new int[1000005];
    public static void generate_rule() throws IOException{
        LineNumberReader reader = new LineNumberReader(new FileReader("***********"));
        LineNumberReader reader1 = new LineNumberReader(new FileReader("***************"));
        String line;
        String[] temp;
        int id=1;
        while((line = reader1.readLine())!=null){

            temp = line.split(",");

            for (int i=1;i<temp.length;i++){
                int k = Integer.parseInt(temp[i]);
                tj[k]++;
            }
        }
        while ((line = reader.readLine()) != null){
            temp = line.split(",");
            Vector<Integer> v = new Vector<>();
            Set<Integer> s1 = new HashSet<>();
            for(int i=0;i<temp.length-1;i++){
                v.add(Integer.parseInt(temp[i]));
                s1.add(Integer.parseInt(temp[i]));
            }
            mp.put(s1,id);
            id++;
        }
    }

}
