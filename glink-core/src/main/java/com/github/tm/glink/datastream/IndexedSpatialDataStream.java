package com.github.tm.glink.datastream;

import com.github.tm.glink.operator.query.RangeQueryOp;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class IndexedSpatialDataStream<T extends Geometry> extends SpatialDataStream<T> {

  private DataStream<T> indexedDataStream;

  public IndexedSpatialDataStream(SpatialDataStream<T> spatialDataStream) {
    indexedDataStream = spatialDataStream.getDataStream()
            .map(r -> {
              Tuple t = (Tuple) r.getUserData();
              Long index = 0L;
              Tuple2<Long, Tuple> indexed = new Tuple2<>(index, t);
              r.setUserData(indexed);
              return r;
            });
  }

  public IndexedSpatialDataStream<T> rangeQuery(Polygon polygon) {
    indexedDataStream.filter(new RangeQueryOp<>(polygon));
    return this;
  }

  public IndexedSpatialDataStream<T> rangeQuery(Envelope spatialWindow) {
    return null;
  }

  public IndexedSpatialDataStream<T> rangeQuery(List<Polygon> queryPolygons) {
    indexedDataStream.filter(new RangeQueryOp<>(queryPolygons));
    return this;
  }

  public SpatialDataStream<T> windowRangeQuery(TimeWindow timeWindow, Envelope spatialWindow) {
    return null;
  }

  public DataStream<T> getDataStream() {
    return indexedDataStream;
  }

  @Override
  public void print() {
    indexedDataStream
            .map(r -> r + " " + r.getUserData())
            .print();
  }
}
