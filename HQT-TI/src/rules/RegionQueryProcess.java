package rules;

import mvs.rule1;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import spatialindex.spatialindex.Region;

import java.io.*;
import java.util.*;

public class RegionQueryProcess {

	/**
	 * @param args
	 * @throws IOException 
	 */

	static DB db = DBMaker.newFileDB(new File("HQT"))
            .closeOnJvmShutdown().
            make();

	static HashMap<Integer, Gt> ObjectTable = new HashMap<Integer, Gt>();
	public static int m=4;
	public static int LastID=0;
	public static long HQTTime=0l;
	public static long processTime=0l;
	//public static long KQTime=0l;
	//public static int d=0;
	public static Vector<Integer> IVs = new Vector<>();
	public static Stack<StackwithInt> S = new Stack<>();
	public static int Sum=0;

	public static Vector<Long> ONs = new Vector<>();


	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		GetHilbert.QuadTree = new HashMap<Long, Node>();
		String location_file = "****";
		String text_file = "****";
		String query_file = "****";
		rule1.generate_rule();
		BufferedReader brl = new BufferedReader(new InputStreamReader(new FileInputStream(location_file)));
		BufferedReader brt = new BufferedReader(new InputStreamReader(new FileInputStream(text_file)));
		BufferedReader brq = new BufferedReader(new InputStreamReader(new FileInputStream(query_file)));
		String str;
		String strt;
		String strq;
		String deterid;
		int count=0;

		Node n = new Node(GetHilbert.global_x1, GetHilbert.global_y1, GetHilbert.global_x2, GetHilbert.global_y2);
//		rules.LRUBuffer.Init();
		while ((str = brl.readLine()) != null)
		{
			String[] temp = str.split(",");
			int id = Integer.parseInt(temp[0]);
			double y = Double.parseDouble(temp[1]);
			double x = Double.parseDouble(temp[2]);
			Gt obj = new Gt(count++, (float)y, (float)x);
			ObjectTable.put(count-1, obj);
			strt = brt.readLine();
			String[] tempt = strt.split(",");
			for(int k = 1; k < tempt.length; k++){
				int term = Integer.parseInt(tempt[k]);
				obj.Keywords.add(term);
			}
			n.GtList.add(obj);
		}

		n.SetBorder();
		GetHilbert.QuadTree.put(0l, n);
		GetHilbert.AssignObjToChildNodes(0l);
	//	rules.GetHilbert.BFS2(0l);
	//	System.out.println(QuadTree.get(28l).GtList.size());


		/* Now is Query Process Section  */

		Vector RF = new Vector<RegionQuery>();

		long start = System.currentTimeMillis();
		int id = -1;
		double[] low = new double[2];
		double[] high = new double[2];
		int id1=0;
		while((strq = brq.readLine()) != null){
			long processSTime = System.nanoTime();
			String[] buf = strq.split(",");
			id = Integer.parseInt(buf[0]);
			System.out.println("query "+id);
			low[0] = Double.parseDouble(buf[1]);
			low[1] = Double.parseDouble(buf[2]);
			high[0] = Double.parseDouble(buf[3]);
			high[1] = Double.parseDouble(buf[4]);
            RegionQuery Rq = new RegionQuery(new Region(low,high));
			Vector<Integer> v = new Vector<>();
			for(int i=5; i<buf.length; i++){
				int word_int = Integer.parseInt(buf[i]);
				v.add(word_int);
			}

			v.sort(new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					if(rule1.tj[o1]>rule1.tj[o2])
						return -1;
					if(rule1.tj[o1]<rule1.tj[o2])
						return 1;
					return 0;
				}
			});
			long processETime = System.nanoTime();
			processTime=processTime+(processETime-processSTime);

			int[] bj=new int[v.size()];

			for(int i=0;i<v.size();i++){
				if(bj[i]==1)
					continue;
				bj[i]=1;
				Set<Integer> s=new HashSet<>();
				s.add(v.get(i));
					for(int j=i+1;j<v.size();j++){
					s.add(v.get(j));
					if(rule1.mp.containsKey(s)==false)
						s.remove(v.get(j));
					else
						bj[j]=1;
				}
				if(rule1.mp.containsKey(s)){
					Rq.Keywords.add(Integer.toString(rule1.mp.get(s)+BuildSFCQ.difKeuword));

				}else{

					Rq.Keywords.add(Integer.toString(v.get(i)));
				}
			}


			/*long startTime = System.nanoTime();
			rules.GetHilbert.FindOidRange(Rq, 0l);
			long endTime = System.nanoTime();
			HQTTime=HQTTime+(endTime-startTime);
			IVs.add(Rq.MinOid);
			IVs.add(Rq.MaxOid);
			Vector<Integer> vector = new Vector<>();
			vector.add(Rq.MinOid);
			vector.add(Rq.MaxOid);
			int i1 = rules.GetHilbert.processVector(vector, Rq);
			System.out.println(i1);*/
/**
 * DFS
 */
         /*
            long lid = rules.GetHilbert.FindOidRange1(Rq, 0l);
			rules.GetHilbert.dfs(lid,0,3,Rq);
			rules.GetHilbert.cutVector(IVs);
			//int vv = rules.GetHilbert.processVector(IVs, Rq);
			//System.out.println(vv);
			System.out.println(IVs);
			IVs.clear();
*/


			long startTime = System.nanoTime();
			long lid = GetHilbert.FindOidRange1(Rq, 0l);
			StackwithInt.push(S, lid, 0);
			double x1 = Rq.region.getLow(0);
			double y1 = Rq.region.getLow(1);
			double x2 = Rq.region.getHigh(0);
			double y2 = Rq.region.getHigh(1);
			while (S.size()!=0){
				//d=d+1;
				StackwithInt pop = S.pop();
				Node node=GetHilbert.QuadTree.get(pop.getNid());
				if (node.Intersect(x1,y1,x2,y2) && (pop.getInteger() == m || node.IsLeaf)){
					if (LastID+1==node.MinOid){
						if (IVs.size()==0) {
							IVs.add(node.MinOid);
							IVs.add(node.MaxOid);
							LastID=node.MaxOid;
						}else {
						    IVs.remove(IVs.size() - 1);
						    IVs.add(node.MaxOid);
						    LastID=node.MaxOid;
						}
					}else{
						IVs.add(node.MinOid);
						IVs.add(node.MaxOid);
						LastID=node.MaxOid;
					}

				} else {
					if (pop.getInteger()==0){
						GetHilbert.FTFO(Rq,pop.getNid());
					}else {
						GetHilbert.FTFOS(Rq,pop.getNid());
					}
					for (int i = ONs.size() - 1; i >= 0; i--) {
						StackwithInt.push(S, ONs.get(i), pop.getInteger()+1);
						//ONs.remove(ONs.get(i));
					}
				}
				ONs.clear();
			}
			long endTime = System.nanoTime();
			HQTTime=HQTTime+(endTime-startTime);
			long startKQTime = System.nanoTime();

			for (int i=0;i<IVs.size();i+=2){
				Sum+= IVs.get(i+1)-IVs.get(i);
			}
			/*int vv = rules.GetHilbert.processVector(IVs, Rq);
			System.out.println(vv);*/
			IVs.clear();
			S.clear();
			LastID=0;
		}
		System.out.println("ID："+Sum/1000);

		/*System.out.println(rules.RegionQuery.ssum/1000);
		long end = System.currentTimeMillis();
		System.out.println("runtime: " + (end - start));
		System.out.println("block accessed: " + rules.RegionQuery.BlockAccess);
		System.out.println("HQT-SQ："+HQTTime/1000/1000);
		System.out.println("deal with："+processTime/1000/1000);
		System.out.println("SLI-KQ："+((end - start)-(HQTTime/1000/1000)-(processTime/1000/1000)));
		System.out.println("ID："+(rules.RegionQuery.ID/10/1000));*/

	}


}
