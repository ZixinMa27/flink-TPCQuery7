package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Orders extends Tuple2<Long, Long> {
    public Long o_orderkey;
    public Long o_custkey;

    public Orders() {
    }
    public Orders(Long orderKey, Long custKey) {
        super(orderKey, custKey);
        this.o_orderkey = orderKey;
        this.o_custkey = custKey;
    }
    public Long getOrderKey() {
        return this.o_orderkey;
    }
    public Long getCustKey() {
        return this.o_custkey;
    }
    public Long getPrimaryKey(){
        return this.o_orderkey;
    }
//    public Long getForeignKey(){
//        return this.o_custkey;
//    }
    public HashMap<String, Long> getForeignKey(){
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Customer", o_custkey);
        return foreignKeyMapping;
    }
}
