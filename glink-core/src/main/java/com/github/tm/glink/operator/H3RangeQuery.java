package com.github.tm.glink.operator;

import com.github.tm.glink.fearures.Point;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.github.tm.glink.index.H3Index;
import com.github.tm.glink.operator.judgement.H3RangeJudgement;
import com.github.tm.glink.operator.judgement.NativeRangeJudgement;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

public class H3RangeQuery {
    public static <T extends Point, U extends Geometry> DataStream<Point> spatialRangeQuery(
            DataStream<Point> geoDataStream,
            U queryGeometry,
            int partitionNum,
            int res) {
        String index = geoDataStream.getExecutionConfig().getGlobalJobParameters().toMap().get("rangeIndex");
        if (index == null || index.equals("null")) {
            return null;
        } else if (index.equals("all")) {
            return null;
        } else if (index.equals("polygon")) {
            return geoDataStream.map(new IndexAssigner(res))
                    .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
                    .filter(new H3RangeJudgement<>(queryGeometry, res));
        } else {
            throw new IllegalArgumentException("Unsupported `rangeIndex`, should be one of the `null`, `polygon`, `all`");
        }
    }

    public static <U extends Geometry> DataStream<Point> spatialRangeQuery(
            DataStream<Point> geoDataStream,
            Envelope queryWindow,
            int partitionNum,
            int res) {
        return geoDataStream.map(new IndexAssigner(res))
                .keyBy(r -> Math.abs(r.getId().hashCode() % partitionNum))
                .filter(new H3RangeJudgement<>(queryWindow, res));
    }
}
