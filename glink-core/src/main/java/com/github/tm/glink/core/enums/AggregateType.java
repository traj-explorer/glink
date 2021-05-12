package com.github.tm.glink.core.enums;

import org.apache.flink.api.common.functions.AggregateFunction;

import java.io.Serializable;

/**
 * @author Wang Haocheng
 * @date 2021/5/8 - 8:56 下午
 */
public enum AggregateType implements Serializable {
    COUNT,
    MAX,
    MIN,
    AVG,
    SUM;

    public static AggregateFunction getAggregateFunction(AggregateType aggregateType, int aggFieldIndex) {
        return null;
    }
}