package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Supplier extends Tuple2<Long, Long> {
    public Long s_suppkey;
    public Long s_nationkey;

    public Supplier() {
    }
    public Supplier(Long suppKey, Long nationKey) {
        super(suppKey, nationKey);
        this.s_suppkey = suppKey;
        this.s_nationkey = nationKey;
    }
    public Long getSuppKey() {
        return this.s_suppkey;
    }
    public Long getNationKey() {
        return this.s_nationkey;
    }
    public Long getPrimaryKey(){
        return this.s_suppkey;
    }
//    public Long getForeignKey(){
//        return this.s_nationkey;
//    }
    public HashMap<String, Long> getForeignKey(){
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Nation1", s_nationkey);
        return foreignKeyMapping;
    }
}
