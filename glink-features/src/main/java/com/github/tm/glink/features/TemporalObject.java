package com.github.tm.glink.features;

import java.time.LocalDateTime;

/**
 * 提供对事件信息获取的能力。
 * @author Wang Haocheng
 * @date 2021/3/3 - 10:32 上午
 */
@Deprecated
public interface TemporalObject {
    public long getTimestamp();
    public void setTimestamp(long timestamp);
    public LocalDateTime getLocalDateTime();
}
