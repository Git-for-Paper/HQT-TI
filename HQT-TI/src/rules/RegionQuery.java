package rules;

import me.lemire.integercompression.IntWrapper;
import mvs.rule1;
import org.mapdb.DB;
import spatialindex.spatialindex.Point;
import spatialindex.spatialindex.Region;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;


public class RegionQuery {

	public static int MinOid;
	public static int MaxOid;
	public static int ID=0;
	public static HashSet<Integer>pst=new HashSet<>();
	public static int BlockAccess = 0;
	public static int ssum = 0;
	public Region region;
	public Vector<String> Keywords;
	public static int limit=6;
	public RegionQuery(Region r){
		this.region = r;
		Keywords = new Vector<String>();
	}

    public ArrayList<Integer> intersectBinary(ArrayList<Integer> list1, ArrayList<Integer> list2) {
	    ArrayList<Integer> res = new ArrayList<Integer>(list1.size());
	    for (int a : list1) {
		    if (binarySearch1(list2, a)) {
			    res.add(a);
		    }
	    }
	    return res;
    }
	public boolean binarySearch1(List<Integer> sortedArray, int value){
		int low = 0;
		int high = sortedArray.size() - 1;
		while(low <= high){
//            ListCount++;
			int mid = (low + high) / 2;
			if(sortedArray.get(mid) < value){
				low = mid + 1;
			}
			else if(sortedArray.get(mid) == value){
				return true;
			}
			else{
				high = mid - 1;
			}
		}
		return false;
	}
	private static <K, V> K reverseLookup(Map<K, V> map, V targetValue) {
		for (Map.Entry<K, V> entry : map.entrySet()) {
			if (entry.getValue().equals(targetValue)) {
				return entry.getKey();
			}
		}
		return null;
	}

	private boolean isSubsetOfLimit2(int[] first, int i, int[] second) {

		int[] subArray = Arrays.copyOfRange(first, i, first.length);

		for (int element : subArray) {
			if (element>41270){
				Set<Integer> set = reverseLookup(rule1.mp, element - 41270);
				int[] intArray = set.stream().mapToInt(Integer::intValue).toArray();
				for (int key:intArray){
					if (binarySearch(second,key)==-1){
						return false;
					}
				}
			}else if (binarySearch(second, element) == -1) {
				return false;
			}
		}
		return true;
	}
	private static int binarySearch(int[] array, int target) {
		Arrays.sort(array);
		int low = 0;
		int high = array.length - 1;

		while (low <= high) {
			int mid = (low + high) / 2;
			if (array[mid] == target) {
				return mid;
			} else if (array[mid] < target) {
				low = mid + 1;
			} else {
				high = mid - 1;
			}
		}
		return -1;
	}
	public Vector<Integer> GetFinalResult(ArrayList[] list, int limit, Vector<String> key) {
		Vector<Integer> Result = new Vector<Integer>();
		Integer[] indices = new Integer[list.length];
		ArrayList<Integer>[] sortList = new ArrayList[list.length];
		for(int i=0;i< list.length;i++){
			indices[i]=i;
		}
		Arrays.sort(indices, Comparator.comparingInt(i -> (list[i] == null) ? 0 : list[i].size()));
		Vector<String> sortKey = new Vector<>(key.size());
		for (int index : indices) {
			sortKey.add(key.get(index));
			sortList[index]=list[indices[index]];

		}
		int minCunt=Math.min(list.length,limit);
		ArrayList<Integer> result = sortList[0];
		for (int i=1;i<minCunt;i++){
			result = intersectBinary(result, sortList[i]);
		}
		if (list.length <= limit && !result.isEmpty()) {
			for (int element:result){
				Gt gt = RegionQueryProcess.ObjectTable.get(element);
				double[] p = new double[2];
				p[0] = gt.olng;
				p[1] = gt.olat;
				if (region.contains(new Point(p))) {
					Result.add(element);
				}
			}
			return Result;
		}
		int[] firstArray=sortKey.stream().mapToInt(Integer::parseInt).toArray();
		for (int i=0;i<result.size();i++){
			Gt gt = RegionQueryProcess.ObjectTable.get(result.get(i));
			Vector<Integer> keywords = gt.Keywords;
			int[] secondArray = keywords.stream().mapToInt(Integer::intValue).toArray();
			if (isSubsetOfLimit2(firstArray,minCunt,secondArray)){
				double[] p = new double[2];
				p[0] = gt.olng;
				p[1] = gt.olat;
				if (region.contains(new Point(p))) {
					Result.add(result.get(i));
					break;
				}
			}

		}

		return Result;
	}
	public static ArrayList[] RemoveData(ArrayList[] set,int k){
		for (int i=0;i<set.length;i++){
			if (i==k){
				continue;
			}
			int lastnumber=set[i].size()-1;
			for (int j=set[i].size()-1;j>0;j--){
				if (!set[i].get(j).equals(set[i].get(j-1))){
					lastnumber=j;
					break;
				}
			}
			if (lastnumber<set[i].size()-1){
				set[i].subList(lastnumber+1,set[i].size()).clear();
			}

		}
		return set;
	}


	private boolean binarySearch(ArrayList<Integer> list, int target) {
		int left = 0;
		int right = list.size() - 1;

		while (left <= right) {
			int mid = left + (right - left) / 2;
			int midVal = list.get(mid);

			if (midVal == target) {
				return true;
			} else if (midVal < target) {
				left = mid + 1;
			} else {
				right = mid - 1;
			}
		}

		return false;
	}


	public int[] uncompress(int[] pagefile){
		int[] recovered = new int[BuildSFCQ.BlockSize];
		IntWrapper recoffset = new IntWrapper(0);
		BuildSFCQ.codec.uncompress(pagefile,new IntWrapper(0),pagefile.length,recovered,recoffset);

		return recovered;
	}

	private static int[] removeDuplicatesFromEnd(Object[] array) {
		Set<Integer> uniqueElements = new LinkedHashSet<>();

		for (Object element : array) {
			uniqueElements.add((Integer) element);
		}
		int[] resultArray = new int[uniqueElements.size()];
		int index = 0;
		for (Integer element : uniqueElements) {
			resultArray[index++] = element;
		}

		return resultArray;
	}

private void processLoop(int startKey, int endKey, int keywordId, ArrayList[] subSet, ConcurrentNavigableMap[] map, HashSet<Integer> block) {
	for (int iKey = startKey; iKey <= endKey; ) {
		if (block.contains(iKey)) {
			if (map[keywordId].higherKey(iKey) == null || (int)map[keywordId].higherKey(iKey) > endKey) {

				break;
			}
			iKey = (int)map[keywordId].higherKey(iKey);
		} else {
			block.add(iKey);

			int[] buf0 = (int[]) map[keywordId].get(iKey);  // uncompress
			int[] buf = uncompress(buf0);

			for (int i = 0; i < buf.length; i++) {
				subSet[keywordId].add(buf[i]);
			}

			if (map[keywordId].higherKey(iKey) == null || (int)map[keywordId].higherKey(iKey) > endKey) {
				break;
			}
			iKey = (int)map[keywordId].higherKey(iKey);
		}
	}
}



	private void fillBlock(int[] minList, int minSizeKeywordId, TreeSet[] pSet,ConcurrentNavigableMap[] map,HashSet[] pblock) {
		for (int i = 0; i < minList.length; i++) {
			int curObj = minList[i];

			for (int j = 0; j < Keywords.size(); j++) {

				if (j == minSizeKeywordId) {
					continue;
				}

				Integer lowerKey = (Integer) map[j].lowerKey(curObj);
				if (lowerKey != null && !pblock[j].contains(lowerKey)) {
					pSet[j].add(lowerKey);
					pblock[j].add(lowerKey);
				}else {
					continue;
				}
			}
		}
	}
	private void Identifykey(int minSizeKeywordId, TreeSet[] pSet, ArrayList[] set,ConcurrentNavigableMap[] map) {
		for (int i = 0; i < Keywords.size(); i++) {
			if (i == minSizeKeywordId) {
				continue;
			}
			Object[] pList = pSet[i].toArray();
			for (int j = 0; j < pList.length; j++) {
				ssum++;
				int curPage = (int) pList[j];
				int[] buf0 = (int[]) map[i].get(curPage);
				int[] buf = uncompress(buf0);
				for (int k = 0; k < buf.length; k++) {
					set[i].add(buf[k]);
				}
			}
		}
	}
	public Vector DAAT(DB db, Vector<Integer> vector){

		ConcurrentNavigableMap[] map = new ConcurrentNavigableMap[Keywords.size()];
		int MinSize = 99999999;
		int MinSizeKeywordId = 99999999;
		float intvisit = 0;

		for(int i=0; i<Keywords.size(); i++){
			map[i] = db.getTreeMap(Keywords.get(i));
			if(map[i].size() < MinSize){//
				MinSize = map[i].size();
				MinSizeKeywordId = i;
			}
		}
		int prevInikey = -1;
		int prevEndkey = -1;
		Vector<Integer> result = new Vector<>();
		HashSet<Integer> block = new HashSet<>();

		HashSet[] Pblock= new HashSet[Keywords.size()];
		for (int j = 0; j < Keywords.size(); j++) {
			Pblock[j] = new HashSet<Integer>();
		}
		for (int i=0;i<vector.size();i+=2){
			int MinOid = vector.get(i);
			int MaxOid = vector.get(i + 1); 
			//inikey = endkey;
			int inikey, endkey ;
			if (map[MinSizeKeywordId].lowerKey(MinOid) != null) {
				inikey = (int) map[MinSizeKeywordId].lowerKey(MinOid);
			} else {
				inikey = (int) map[MinSizeKeywordId].firstKey();
			}
			if (map[MinSizeKeywordId].lowerKey(MaxOid) != null) {
				endkey = (int) map[MinSizeKeywordId].lowerKey(MaxOid);
			} else {
				endkey = (int) map[MinSizeKeywordId].firstKey();
			}

			if (inikey == prevInikey && endkey == prevEndkey) {
				BlockAccess += (intvisit / BuildSFCQ.BlockSize + 1);
				continue;
			}
			prevInikey = inikey;
			prevEndkey = endkey;
			ArrayList[] set = new ArrayList[Keywords.size()];
			TreeSet[] pset = new TreeSet[Keywords.size()];
			for (int j = 0; j < Keywords.size(); j++) {
				set[j] = new ArrayList();
				pset[j] = new TreeSet<Integer>();
			}
			processLoop(inikey, endkey, MinSizeKeywordId, set, map,block);
			BlockAccess += (intvisit / BuildSFCQ.BlockSize + 1);

			int[] delettail1 = removeDuplicatesFromEnd(set[MinSizeKeywordId].toArray());

			fillBlock(delettail1, MinSizeKeywordId, pset, map,Pblock);
			Identifykey(MinSizeKeywordId, pset, set, map);
			for (int i1 = 0; i1 < set.length; i1++) {
				 ID=ID+set[i1].size();
			}
			Vector<Integer> res = GetFinalResult(set, limit, Keywords);
			result.addAll(res);
		}
		return result;

	}

}
