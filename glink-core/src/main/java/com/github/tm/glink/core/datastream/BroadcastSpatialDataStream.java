package com.github.tm.glink.core.datastream;


import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.source.GeoMesaGeometrySourceFunction;
import com.github.tm.glink.connector.geomesa.util.GeoMesaTableSchema;
import com.github.tm.glink.connector.geomesa.util.GeoMesaType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class BroadcastSpatialDataStream<T extends Geometry> {

  private DataStream<Tuple2<Boolean, T>> dataStream;

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final SourceFunction<Tuple2<Boolean, T>> sourceFunction) {
    dataStream = env.addSource(sourceFunction);
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String path,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    dataStream = env
            .readTextFile(path)
            .flatMap(flatMapFunction);
  }

  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    final String host,
                                    final int port,
                                    final FlatMapFunction<String, Tuple2<Boolean, T>> flatMapFunction) {
    dataStream = env
            .socketTextStream(host, port)
            .flatMap(flatMapFunction);
  }

  /**
   * @param env
   * @param externalName 外部存储类型，目前仅支持Geomesa。
   * @param fieldNamesToTypes 应与对应的Geomesa Schema相同。
   * @param configuration  需要在其中指定PRIMARY_FIELD_NAME，目前仅支持一个Field作为 primary。
   */
  public BroadcastSpatialDataStream(final StreamExecutionEnvironment env,
                                    String externalName,
                                    List<Tuple2<String, GeoMesaType>> fieldNamesToTypes,
                                    Configuration configuration) {
    if (externalName.equalsIgnoreCase("Geomesa-HBase")) {
      // GeoMesa table schema
      GeoMesaTableSchema geoMesaTableSchema = GeoMesaTableSchema.fromFieldNamesAndTypes(fieldNamesToTypes, configuration);
      // DataStore相关信息：schema，catalog等
      GeoMesaDataStoreParam geoMesaDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
      geoMesaDataStoreParam.initFromConfigOptions(configuration);
      dataStream = env
              .addSource(new GeoMesaGeometrySourceFunction<T>(geoMesaDataStoreParam, geoMesaTableSchema), TypeInformation.<T>of((Class<T>)Geometry.class))
              .flatMap(new FlatMapFunction<T, Tuple2<Boolean, T>>() {
                @Override
                public void flatMap(T value, Collector<Tuple2<Boolean, T>> out) throws Exception {
                  out.collect(new Tuple2<Boolean, T>(true, value));
                }
              });
    } else {
      throw new IllegalArgumentException("Only supprot Geomesa-HBase as an external database soure right now");
    }
  }

  public DataStream<Tuple2<Boolean, T>> getDataStream() {
    return dataStream;
  }
}
