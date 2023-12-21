import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import scala.reflect.internal.ReificationSupport;

import java.io.IOException;
import java.util.*;

public class ProcessQueryFunctionNew extends ProcessFunction<Update, List<Tuple4<String, String, Integer, Double>>>{
    private MapState<Long, Tuple4<String, String, Integer, Double>>  joinResultState;

    private MapState<String, TableLogger> tableState;

    private MapState<Tuple3<String,String,Integer>, Double> calculationState;

    @Override
    public void open(Configuration parameters) throws Exception {

        joinResultState =  getRuntimeContext().getMapState(
                new MapStateDescriptor<>("ResultState", Types.LONG, Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE)));

        tableState =  getRuntimeContext().getMapState(
                new MapStateDescriptor<>("AllTableState",String.class, TableLogger.class));

        calculationState =  getRuntimeContext().getMapState(
                new MapStateDescriptor<>("calculationState", Types.TUPLE(Types.STRING, Types.STRING, Types.INT), Types.DOUBLE));
    }

    @Override
    public void processElement(Update updateTuple,
                               Context context,
                               Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {


        if(tableState.isEmpty()){
            // initialize table log
            System.out.println("Start Streaming");
            TableLogger lineitemLog = new TableLogger("Lineitem");
            TableLogger ordersLog = new TableLogger("Orders");
            TableLogger customerLog = new TableLogger("Customer");
            TableLogger supplierLog = new TableLogger("Supplier");
            TableLogger nationLog1 = new TableLogger("Nation1");
            TableLogger nationLog2 = new TableLogger("Nation2");


            // initial set
            // Lineitem
            lineitemLog.initialSet(true, false, 2, "",
                    new ArrayList<>(Arrays.asList("Orders", "Supplier")), new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Orders
            ordersLog.initialSet(false, false, 1, "Lineitem",
                    new ArrayList<>(List.of("Customer")),new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Customer
            customerLog.initialSet(false, false, 1, "Orders",
                    new ArrayList<>(List.of("Nation2")),new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Supplier
            supplierLog.initialSet(false, false, 1, "Lineitem",
                    new ArrayList<>(List.of("Nation1")),new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Nation1
            nationLog1.initialSet(false, true, 0, "Supplier",
                    new ArrayList<>(),new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Nation2
            nationLog2.initialSet(false, true, 0, "Customer",
                    new ArrayList<>(),new Hashtable<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            tableState.put("Lineitem", lineitemLog);
            tableState.put("Orders", ordersLog);
            tableState.put("Customer", customerLog);
            tableState.put("Supplier", supplierLog);
            tableState.put("Nation1", nationLog1);
            tableState.put("Nation2", nationLog2);
        }

        insertAlgorithm(tableState, updateTuple, collector);
        deleteAlgorithm(tableState, updateTuple, collector);


        Iterable<Tuple4<String, String, Integer, Double>> joinState = joinResultState.values();
        for (Tuple4<String, String, Integer, Double> joinResult : joinState) {
            if (!joinResult.f0.equals(joinResult.f1)) {
                Tuple3<String, String, Integer> keyBy = new Tuple3<>(joinResult.f0, joinResult.f1, joinResult.f2);
                if (calculationState.get(keyBy)!= null){
                    double curSum = calculationState.get(keyBy);
                    calculationState.put(keyBy, curSum+joinResult.f3);
                }
                else{
                    calculationState.put(keyBy,joinResult.f3);
                }

            }
        }

        List<Tuple4<String, String, Integer, Double>> output = new ArrayList<>();
        for (Map.Entry<Tuple3<String, String, Integer>, Double> entry : calculationState.entries()) {
            Tuple3<String, String, Integer> key = entry.getKey();
            Double val = entry.getValue();
            output.add(new Tuple4<>(key.f0, key.f1, key.f2, val));
        }
        collector.collect(output);
        calculationState.clear();
        //System.out.println(joinState);

    }

    public void insertAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        if (updateTuple.operation.compareTo("+") == 0) {
            //System.out.println("process table is " + updateTuple.tableName + " insert ");
            TableLogger tupleTable = allTableState.get(updateTuple.tableName);
            tupleTable.allTuples.put(updateTuple.primaryKey, updateTuple);
            // if R is not a leaf then
            if (!tupleTable.isLeaf) {
                // s <- 0 initialize this tuple counter, use its primary key as index
                Hashtable<Long, Integer> sCounter = tupleTable.getsCounter();
                sCounter.put(updateTuple.primaryKey, 0);
                // for each Rc ∈ C(R) for each child table
                for (String childName : tupleTable.childInfo) {
                    //I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
                    // initialize I(R,Rc) if null
                    tupleTable.indexTableAndTableChildInfo.computeIfAbsent(childName, k -> new HashMap<Long, ArrayList<Long>>());
                    Long tupleForeignKey = updateTuple.foreignKeyMapping.get(childName);
                    // initialize an arraylist if not exist the key
                    ArrayList<Long> lst =  tupleTable.indexTableAndTableChildInfo.get(childName).get(tupleForeignKey);
                    if(lst!=null) {
                        if (lst.contains(updateTuple.primaryKey)) {
                            throw new Exception("Should not have same primary key. Assign Lineitem Unique primary Key");
                        }
                        lst.add(updateTuple.primaryKey);
                    }else{
                        tupleTable.indexTableAndTableChildInfo.get(childName).put(tupleForeignKey, new ArrayList<>(List.of(updateTuple.primaryKey)));
                    }
                    // if πPK(Rc)t ∈ I(Rc) then s(t) ← s(t) + 1
                    // if this tuple foreign key appear in child table live tuple set, increase tuple counter by 1
                    if (allTableState.get(childName).indexLiveTuple.containsKey(tupleForeignKey)) {
                        int curCount = sCounter.get(updateTuple.primaryKey);
                        sCounter.put(updateTuple.primaryKey, curCount + 1);
                    }
                }
                //update assertion key, q7 has no assertion key
            }
            // if R is a leaf or s(t) = |C(R)|  then
            if (tupleTable.isLeaf || (tupleTable.getsCounter().get(updateTuple.primaryKey) == Math.abs(tupleTable.numChild))) {
                // insert-Update(t, R,t) Algo
                insertUpdateAlgorithm(tableState, updateTuple, collector);
            }

            // else I(N(R)) ← I(N(R)) + (πPK(R)t → t)  put this tuple to non live tuple set.
            else {
                tupleTable.indexNonLiveTuple.put(updateTuple.primaryKey, updateTuple);
            }
        }
    }

    public void insertUpdateAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        //I(L(R)) ← I(L(R)) + (πPK(R)t → t) // put this tuple to live tuple set
        //System.out.println("process insert update");
        TableLogger tupleTable = allTableState.get(updateTuple.tableName);
        tupleTable.indexLiveTuple.put(updateTuple.primaryKey, updateTuple);
        //if R is the root of T then ∆Q ← ∆Q ∪ {join_result }
        if (tupleTable.isRoot && (tupleTable.sCounter.get(updateTuple.primaryKey) == tupleTable.numChild)) {
            //Perform Join
            //System.out.println("Insert Tuple " + updateTuple.toString());
            Tuple4<String, String, Integer,Double> result = getSelectedTuple(allTableState, updateTuple.primaryKey);
            joinResultState.put(updateTuple.primaryKey, result);

        }
        //else P ← look up I(Rp, R) with key πPK(R)t
        else{
            TableLogger parentTable = allTableState.get(tupleTable.parentName);
            HashMap<Long, ArrayList<Long>> IRpAndR = parentTable.indexTableAndTableChildInfo.get(tupleTable.tableName);
            if (IRpAndR != null){
                ArrayList<Long> parentPKey = IRpAndR.get(updateTuple.primaryKey);
                //for each tp ∈ P do
                if (parentPKey!=null) {
                    for (Long tp : parentPKey) {

                        //s(tp ) ← s(tp ) + 1
                        int curCount = parentTable.sCounter.get(tp);
                        parentTable.sCounter.put(tp, curCount + 1);
                        //if s(tp ) = |C(Rp )| then

                        if (parentTable.sCounter.get(tp) == Math.abs(parentTable.numChild)) {
                            // I(N(Rp )) ← I(N(Rp )) − (πPK(Rp ) tp → tp ) update non live tuple set
                            if(parentTable.allTuples.containsKey(tp)){
                                Update parentTuple = parentTable.allTuples.get(tp);
                                parentTable.indexNonLiveTuple.remove(tp);
                                // Insert-Update(tp, Rp, join_result tp )
                                insertUpdateAlgorithm(tableState, parentTuple, collector);
                            }
                        }
                    }
                }
            }
        }
    }

    public void deleteAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // delete
        if (updateTuple.operation.compareTo("-") == 0) {
           // System.out.println("process table is " + updateTuple.tableName + " delete ");
            TableLogger tupleTable = allTableState.get(updateTuple.tableName);
            tupleTable.allTuples.remove(updateTuple.primaryKey);
            //if t ∈ L(R) then  if it is in live tuple set
            if  (tupleTable.indexLiveTuple.containsKey(updateTuple.primaryKey)){
                deleteUpdateAlgorithm(allTableState, updateTuple,  collector);
            }
            else{
                // remove this tuple from non live tuple set
                tupleTable.indexNonLiveTuple.remove(updateTuple.primaryKey);
            }

            // if table is not the root
            if(!tupleTable.isRoot) {
                // I(Rp, R) ← I(Rp, R) − (πPK(R)t → πPK(Rp ),PK(R)t)
                TableLogger parentTable = allTableState.get(tupleTable.parentName);
                HashMap<Long, ArrayList<Long>> IRpAndR = parentTable.indexTableAndTableChildInfo.get(tupleTable.tableName);
                if (IRpAndR!=null) {
                    IRpAndR.remove(updateTuple.primaryKey);
                }
            }
        }
    }
    public void deleteUpdateAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // remove from live tuple
        TableLogger tupleTable = allTableState.get(updateTuple.tableName);
        tupleTable.indexLiveTuple.remove(updateTuple.primaryKey);
        if (tupleTable.isRoot){
            //delete from join
            //System.out.println("delete Tuple " + tupleTable.toString());
            joinResultState.remove(updateTuple.primaryKey);
        }
        else{
            //P ← look up I(Rp, R) with key πPK(R)t
            TableLogger parentTable = allTableState.get(tupleTable.parentName);
            HashMap<Long, ArrayList<Long>> IRpAndR = parentTable.indexTableAndTableChildInfo.get(tupleTable.tableName);
            if (IRpAndR!= null){
                ArrayList<Long> parentPKey = IRpAndR.get(updateTuple.primaryKey);
                //for each tp ∈ P do
                if(parentPKey!= null){
                    for (Long tp : parentPKey) {
                        //if tp ∈ N(Rp ) then
                        if (parentTable.indexNonLiveTuple.containsKey(tp)) {
                            //s(tp ) ← s(tp ) - 1
                            int curCount = parentTable.sCounter.get(tp);
                            parentTable.sCounter.put(tp, curCount - 1);

                        } else {
                            //s(tp ) ← |C(R)| − 1
                            parentTable.sCounter.put(tp, Math.abs(tupleTable.numChild) - 1);

                            //I(N(Rp )) ← I(N(Rp )) + (πPK(Rp)t → t) update non live tuple set
//                            Update parentTuple = parentTable.indexLiveTuple.get(tp);
//                            parentTable.indexNonLiveTuple.put(tp, parentTuple);
//                            deleteUpdateAlgorithm(allTableState, parentTuple, collector);
                            if(parentTable.allTuples.containsKey(tp)){
                                Update parentTuple = parentTable.allTuples.get(tp);
                                parentTable.indexNonLiveTuple.put(tp, parentTuple);
                                // Delete-Update(tp, Rp, join_result tp )
                                deleteUpdateAlgorithm(allTableState,parentTuple, collector);
                            }

                        }
                    }
                }
            }

        }
    }

    public Tuple4<String, String,Integer, Double> getSelectedTuple(MapState<String, TableLogger> allTableState, Long LineitemPKey) throws Exception {
        Long l_orderkey = allTableState.get("Lineitem").indexLiveTuple.get(LineitemPKey).lineitemTuple.l_orderkey;
        Long l_suppkey = allTableState.get("Lineitem").indexLiveTuple.get(LineitemPKey).lineitemTuple.l_suppkey;
        Date l_shipdate = allTableState.get("Lineitem").indexLiveTuple.get(LineitemPKey).lineitemTuple.l_shipDate;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(l_shipdate);
        int l_shipYear = calendar.get(Calendar.YEAR);
        Double l_extendedPrice = allTableState.get("Lineitem").indexLiveTuple.get(LineitemPKey).lineitemTuple.l_extendedPrice;
        Double l_discount = allTableState.get("Lineitem").indexLiveTuple.get(LineitemPKey).lineitemTuple.l_discount;
        Long s_nationkey = allTableState.get("Supplier").indexLiveTuple.get(l_suppkey).supplierTuple.s_nationkey;
        String n_name1 = allTableState.get("Nation1").indexLiveTuple.get(s_nationkey).nationTuple.n_name;
        Long o_custkey = allTableState.get("Orders").indexLiveTuple.get(l_orderkey).ordersTuple.o_custkey;
        Long c_nationkey = allTableState.get("Customer").indexLiveTuple.get(o_custkey).customerTuple.c_nationkey;
        String n_name2 = allTableState.get("Nation2").indexLiveTuple.get(c_nationkey).nationTuple.n_name;
        Double volumn  = l_extendedPrice * (1 - l_discount);
        return Tuple4.of(n_name1,n_name2, l_shipYear, volumn);
    }


}
