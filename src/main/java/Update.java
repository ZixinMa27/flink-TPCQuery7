import org.apache.flink.api.java.tuple.Tuple3;

import Tables.*;

import java.util.ArrayList;
import java.util.HashMap;

public class Update extends Tuple3<String,String,Object>{
    public String operation;
    public String tableName;
    public Long primaryKey;
    public Customer customerTuple;
    public Lineitem lineitemTuple;
    public Nation nationTuple;
    public Orders ordersTuple;
    public Supplier supplierTuple;
    public HashMap<String, Long> foreignKeyMapping;
    public String someKey = "same";


    public Update(){
    }

    public Update(String operation, String tableName, Customer tuple) {
        super(operation, tableName, tuple);
        this.operation = operation;
        this.tableName = tableName;
        this.customerTuple = tuple;
        this.primaryKey = tuple.getPrimaryKey();
        this.foreignKeyMapping = tuple.getForeignKey();
    }

    public Update(String operation, String tableName, Lineitem tuple) {
        super(operation, tableName, tuple);
        this.operation = operation;
        this.tableName = tableName;
        this.lineitemTuple = tuple;
        this.primaryKey = tuple.getPrimaryKey();
        this.foreignKeyMapping = tuple.getForeignKey();
    }

    public Update(String operation, String tableName,  Nation tuple) {
        super(operation, tableName, tuple);
        this.operation = operation;
        this.tableName = tableName;
        this.nationTuple = tuple;
        this.primaryKey = tuple.getPrimaryKey();
        this.foreignKeyMapping = tuple.getForeignKey();
    }

    public Update(String operation, String tableName,  Orders tuple) {
        super(operation, tableName, tuple);
        this.operation = operation;
        this.tableName = tableName;
        this.ordersTuple = tuple;
        this.primaryKey = tuple.getPrimaryKey();
        this.foreignKeyMapping = tuple.getForeignKey();
    }
    public Update(String operation, String tableName,  Supplier tuple) {
        super(operation, tableName, tuple);
        this.operation = operation;
        this.tableName = tableName;
        this.supplierTuple = tuple;
        this.primaryKey = tuple.getPrimaryKey();
        this.foreignKeyMapping = tuple.getForeignKey();
    }

    @Override
    public String toString() {
        if (tableName.compareTo("Customer") == 0) {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", customerTuple=" + customerTuple +
                    '}';
        } else if (tableName.compareTo("Lineitem") == 0) {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", lineitemTuple=" + lineitemTuple +
                    '}';
        } else if (tableName.compareTo("Nation1") == 0) {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", nationTuple=" + nationTuple +
                    '}';

        } else if (tableName.compareTo("Nation2") == 0) {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", nationTuple=" + nationTuple +
                    '}';
        } else if (tableName.compareTo("Orders") == 0) {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", ordersTuple=" + ordersTuple +
                    '}';
        } else {
            return "Update{" +
                    "operation='" + operation + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", primaryKey=" + primaryKey +
                    ", supplierTuple=" + supplierTuple +
                    '}';
        }


    }

}
