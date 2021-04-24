package com.github.tm.glink.features;

/**
 * 提供对事件信息获取的能力。
 * @author Wang Haocheng
 * @date 2021/3/3 - 10:32 上午
 */
@Deprecated
public interface TemporalObject {
    public long getTimestamp();
    public long setTimestamp(long timestamp);
}
