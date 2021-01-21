package com.github.tm.glink.examples.hbase;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

public class HBaseTemporalTableJoin {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Order> orderDataStream =
                env.fromCollection(
                        Arrays.asList(
                                new Order(1L, 12.2, "USD", System.currentTimeMillis()),
                                new Order(2L, 300, "YEN", System.currentTimeMillis()),
                                new Order(3L, 10, "GBP", System.currentTimeMillis())));

        Table orders = tEnv.fromDataStream(orderDataStream, $("order_id"), $("price"), $("currency"),
                $("time"));

        tEnv.executeSql("CREATE TABLE rates (\n"
                + "    currency STRING,\n"
                + "    f ROW<rate DECIMAL(32, 2)>\n"
                + ") WITH (\n"
                + "    'connector' = 'hbase-1.4',\n"
                + "    'table-name' = 'rates',\n"
                + "    'zookeeper.quorum' = 'localhost:2181',\n"
                + "    'zookeeper.znode.parent' = '/hbase'\n"
                + ")");

        Table result = tEnv.sqlQuery("SELECT * FROM rates");

        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /** Simple POJO. */
    public static class Order {
        public long order_id;
        public double price;
        public String currency;
        public long time;

        public Order() {}

        public Order(long order_id, double price, String currency, long time) {
            this.order_id = order_id;
            this.price = price;
            this.currency = currency;
            this.time = time;
        }

        @Override
        public String toString() {
            return "Order{"
                    + "order_id="
                    + order_id
                    + ", price='"
                    + price
                    + '\''
                    + ", currency="
                    + currency
                    + '}';
        }
    }
}

