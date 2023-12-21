import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class TableLogger {
    public String tableName;
    public boolean isRoot;
    public boolean isLeaf;
    public int numChild;

    public String parentName;
    public ArrayList<String> childInfo;
    public Hashtable<Long, Update> allTuples;
    public Hashtable<Long, Update> indexLiveTuple; // I(L(R)) leaf R, (key,value) = (pKey, live tuple)
    public Hashtable<Long, Update> indexNonLiveTuple; // I(N(R)) non-leaf R, (key,value) = (pKey, non-live tuple)
    public Hashtable<Long, Integer> sCounter; // for non-leaf R, (key,value) = (pKey, num of child of this tuple is alive)
    public Hashtable<String, HashMap<Long, ArrayList<Long>>> indexTableAndTableChildInfo; //(key, value) = childName, thisTableAndTableChildInfo


    public TableLogger(){
    }

    public TableLogger(String tableName){
        this.tableName = tableName;
    }

    public void initialSet(boolean isRoot, boolean isLeaf, int numChild,
                           String parentName, ArrayList<String> childInfo, Hashtable<Long, Update> allTuples,
                           Hashtable<Long, Update> indexLiveTuple, Hashtable<Long, Update> indexNonLiveTuple,
                           Hashtable<Long, Integer> sCounter, Hashtable<String, HashMap<Long, ArrayList<Long>>> indexTableAndTableChildInfo){
        this.isRoot = isRoot;
        this.isLeaf = isLeaf;
        this.numChild = numChild;
        this.parentName = parentName;
        this.childInfo = childInfo;
        this.indexLiveTuple = indexLiveTuple;
        this.indexNonLiveTuple = indexNonLiveTuple;
        this.sCounter = sCounter;
        this.indexTableAndTableChildInfo = indexTableAndTableChildInfo;
        this.allTuples = allTuples;
    }

    public Hashtable<Long, Integer> getsCounter() {
        return this.sCounter;
    }

    @Override
    public String toString() {
        return "TableLogger{" +
                "tableName='" + tableName + '\'' +
                ", allTuples=" + allTuples +
                ", indexLiveTuple=" + indexLiveTuple +
                ", indexNonLiveTuple=" + indexNonLiveTuple +
                ", sCounter=" + sCounter +
                ", indexTableAndTableChildInfo=" + indexTableAndTableChildInfo +
                '}';
    }
//    public static class TableAndTableChild {
//        public String table;
//        public String tableChild;
//        public HashMap<Long, ArrayList<Long>> indexTableAndTableChild;
//
//        public TableAndTableChild(String table, String tableChild) {
//            this.table = table;
//            this.tableChild = tableChild;
//            this.indexTableAndTableChild = new HashMap<>();
//        }
//
//    }
}



