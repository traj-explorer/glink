package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

/**
 * Parse a millisecond value to java.sql.timestamp.
 * @author Wang Haocheng
 * @date 2021/3/10 - 10:20 下午
 */
public class ParseTimestamp extends ScalarFunction{
    public Timestamp eval(Long s) {
        return new Timestamp(s);
    }
}
