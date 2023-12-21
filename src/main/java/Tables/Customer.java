package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Customer extends Tuple2<Long, Long> {
    public Long c_custkey;
    public Long c_nationkey;

    public Customer() {
    }
    public Customer(Long custKey, Long nationKey) {
        super(custKey, nationKey);
        this.c_custkey = custKey;
        this.c_nationkey = nationKey;
    }
    public Long getCustKey() {
        return this.c_custkey;
    }
    public Long getNationKey() {
        return this.c_nationkey;
    }

    public Long getPrimaryKey(){
        return this.c_custkey;
    }

    public HashMap<String, Long> getForeignKey(){
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Nation2", c_nationkey);
        return foreignKeyMapping;
    }

//    public Long getForeignKey(){
//        return this.c_nationkey;
//    }

}
