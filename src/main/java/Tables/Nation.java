package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Nation extends Tuple2<Long, String> {
    public Long n_nationkey;
    public String n_name;

    public Nation() {
    }
    public Nation(Long nationkey, String name) {
        super(nationkey, name);
        this.n_nationkey = nationkey;
        this.n_name = name;
    }
    public Long getNationKey(){
        return this.n_nationkey;
    }
    public String getName(){
        return this.n_name;
    }
    public Long getPrimaryKey(){
        return this.n_nationkey;
    }

    //leaf
    public HashMap<String, Long> getForeignKey(){
        return new HashMap<String, Long>();
    }
}
