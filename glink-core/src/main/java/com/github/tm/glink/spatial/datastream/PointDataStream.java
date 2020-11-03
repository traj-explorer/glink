package com.github.tm.glink.spatial.datastream;

import com.github.tm.glink.enums.TextFileSplitter;
import com.github.tm.glink.format.PointTextFormatMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class PointDataStream extends SpatialDataStream<Point> {

  public PointDataStream(
          StreamExecutionEnvironment env,
          int geometryOffset,
          TextFileSplitter splitter,
          boolean carryAttributes,
          String... elements) {
    super(env, geometryOffset, splitter, carryAttributes, elements);
    spatialDataStream = env
            .fromElements(elements)
            .flatMap(new PointTextFormatMap(geometryOffset, splitter, carryAttributes));
  }

  @Override
  public PointDataStream rangeQuery(Envelope rangeQueryWindow) {
    spatialDataStream = spatialDataStream.filter(new FilterFunction<Point>() {
      @Override
      public boolean filter(Point point) throws Exception {
        return rangeQueryWindow.contains(point.getCoordinate());
      }
    });
    return this;
  }
}
