import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.sql.Date;
import java.util.List;

public class RunQ7 {
    public static void main(String[] args) throws Exception {

        /*
           创建执行环境 set up the execution environment
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //env.setParallelism(1);

        /*
            读取数据创建 get input data ( 5 tables) and collect tuple info
         */
        DataStream<Update> dataSource = env.addSource(new DataSource());

        /*
            数据处理 transformation
         */
        // Condition Filter
        String NATION1 = "GERMANY";
        String NATION2 = "FRANCE";
        dataSource =
                dataSource.filter((FilterFunction<Update>) data -> {
                    if (data.tableName.compareTo("Lineitem") == 0) {
                        return !data.lineitemTuple.l_shipDate.before(Date.valueOf("1995-01-01")) // l_shipdate >= date '1995-01-01'
                                && !data.lineitemTuple.l_shipDate.after(Date.valueOf("1996-12-31")); // and l_shipdate <= date '1996-12-31'
                    } else if (data.tableName.compareTo("Nation1") == 0) { // (n1.n_name = '[NATION1]' or n1.n_name = '[NATION2]')
                        return data.nationTuple.n_name.compareTo(NATION1) == 0 || data.nationTuple.n_name.compareTo(NATION2) == 0;
                    } else if (data.tableName.compareTo("Nation2") == 0) { // (n2.n_name = '[NATION1]' or n2.n_name = '[NATION2]')
                        return data.nationTuple.n_name.compareTo(NATION1) == 0 || data.nationTuple.n_name.compareTo(NATION2) == 0;
                    }
                    return true;
                });

        // Process Query insert/delete
        DataStream<List<Tuple4<String, String, Integer, Double>>>
                result= dataSource.keyBy("someKey")
                .process(new ProcessQueryFunctionNew());

        /*
            数据输出 sink
         */
        // Write to text file
        //result.writeAsText("test.txt");
        result.print();

        /*
            启动任务 execute
         */
        final Instant start = Instant.now();
        System.out.println("TPC-H Query7 Flink DataStream - start");
        env.execute();
        final Instant end = Instant.now();
        final Duration runtime = Duration.between(start, end);
        System.out.printf(
                "TPC-H %s - end - %d m %d s. Total: %d%n",
                "Query 7",
                runtime.toMinutes(),
                runtime.getSeconds() % 60,
                runtime.getSeconds()
        );

    }


}

