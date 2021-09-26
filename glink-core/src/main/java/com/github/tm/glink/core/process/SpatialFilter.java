package com.github.tm.glink.core.process;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.geom.MultiPolygonWithIndex;
import com.github.tm.glink.core.geom.PolygonWithIndex;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.util.function.Predicate;

/**
 * The spatial filter class.
 */
public class SpatialFilter {

  public static <T extends Geometry, U extends Geometry> DataStream<T> filter(
          SpatialDataStream<T> spatialDataStream,
          U geometry,
          TopologyType topologyType) {
    return spatialDataStream.getDataStream().filter(new RichFilterFunction<T>() {

      private transient Predicate<T> predicate;

      @Override
      public void open(Configuration parameters) throws Exception {
        Geometry filerGeometry = geometry;
        if (geometry instanceof Polygon) {
          filerGeometry = PolygonWithIndex.fromPolygon((Polygon) geometry);
        } else if (geometry instanceof MultiPolygon) {
          filerGeometry = MultiPolygonWithIndex.fromMultiPolygon((MultiPolygon) geometry);
        }
        Geometry finalFilerGeometry = filerGeometry;
        switch (topologyType) {
          case P_CONTAINS:
            predicate = (g) -> g.contains(finalFilerGeometry);
            break;
          case N_CONTAINS:
            predicate = finalFilerGeometry::contains;
            break;
          case INTERSECTS:
            predicate = finalFilerGeometry::intersects;
            break;
          default:
            throw new IllegalArgumentException("Unsupported topology type");
        }
      }

      @Override
      public boolean filter(T g) throws Exception {
        return predicate.test(g);
      }
    });
  }
}
