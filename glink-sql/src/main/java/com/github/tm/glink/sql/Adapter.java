package com.github.tm.glink.sql;

import com.github.tm.glink.sql.util.Schema;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public class Adapter implements Serializable {

  public static <T extends Geometry> Table toTable(
          final StreamTableEnvironment tEnv,
          final SpatialDataStream<T> spatialDataStream,
          final Expression[] filesName,
          final Class<?>[] fieldsType) {
    DataStream<Row> rowDataStream = spatialDataStream.getDataStream().map(r -> {
      Tuple attributes = (Tuple) r.getUserData();
      Object[] rowItems = new Object[attributes.getArity() + 1];
      rowItems[0] = r;
      for (int i = 0, len = attributes.getArity(); i < len; ++i) {
        rowItems[i + 1] = attributes.getField(i);
      }
      return Row.of(rowItems);
    }).returns(new RowTypeInfo(Schema.toFlinkTypes(fieldsType)));
    return tEnv.fromDataStream(rowDataStream, filesName);
  }
}
