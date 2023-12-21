package Tables;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

import java.sql.Date;
import java.util.HashMap;

public class Lineitem extends Tuple6<Long, Long, Long, Double, Double, Date> {

    public Long l_orderkey;
    public Long l_suppkey;
    public Long l_lineNumber;
    public Double l_extendedPrice;
    public Double l_discount;
    public Date l_shipDate;

    public Lineitem() {
    }

    public Lineitem(Long orderKey, Long suppKey, Long lineNumber, Double extendedPrice, Double discount, Date shipDate) {
        super(orderKey, suppKey, lineNumber, extendedPrice, discount, shipDate);
        this.l_orderkey = orderKey;
        this.l_suppkey = suppKey;
        this.l_lineNumber = lineNumber;
        this.l_extendedPrice = extendedPrice;
        this.l_discount = discount;
        this.l_shipDate = shipDate;

    }

    public Long getOrderKey() {
        return l_orderkey;
    }
    public Long getSuppKey() {
        return l_suppkey;
    }
    public Long getLineNumber(){return l_lineNumber;}
    public Double getExtendedPrice() {
        return l_extendedPrice;
    }
    public Double getDiscount() {
        return l_discount;
    }
    public Date getShipDate(){return l_shipDate;}

    public Long getPrimaryKey(){
        // the relation lineitem Primary key has a composite foreign key (l_suppkey,l_linenumber)
        //long value = (long) (l_orderkey * 10000+ this.l_suppkey * 10000 + l_lineNumber * 10000 + l_extendedPrice *1000+ l_discount * 1000) ;
        //return Long.parseLong(Long.toString(value).concat(this.l_lineNumber.toString()));
        //return l_orderkey * (10^9 + 7) + l_lineNumber;
        Long a = l_orderkey;
        Long b = l_lineNumber;
        return  a >= b ? a * a + a + b : a + b * b;
    }

    public HashMap<String, Long> getForeignKey(){
        // lineitem has two children, so join on different foreign key
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Supplier", l_suppkey );
        foreignKeyMapping.put("Orders", l_orderkey);
        return foreignKeyMapping;
    }

}
