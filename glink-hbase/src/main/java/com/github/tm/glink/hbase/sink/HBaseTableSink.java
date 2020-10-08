package com.github.tm.glink.hbase.sink;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.avro.AvroPoint;
import com.github.tm.stindex.ByteArray;
import com.github.tm.stindex.dimension.BasicDimensionDefinition;
import com.github.tm.stindex.dimension.TimeDimensionDefinition;
import com.github.tm.stindex.spatial.GridIndex;
import com.github.tm.stindex.spatial.sfc.SFCDimensionDefinition;
import com.github.tm.stindex.spatial.sfc.SFCFactory;
import com.github.tm.stindex.st.ConcatenationEncoding;
import com.github.tm.stindex.st.STEncoding;
import com.github.tm.stindex.temporal.ConcatenationTimeEncoding;
import com.github.tm.stindex.temporal.TimeEncoding;
import com.github.tm.stindex.temporal.TimePointDimensionDefinition;
import com.github.tm.stindex.temporal.data.TimeValue;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;

/**
 * @author Yu Liebing
 * */
public class HBaseTableSink<T extends Point> extends RichSinkFunction<T> {

  private transient Configuration configuration;
  private transient Connection connection;
  private transient Admin admin;

  private transient Table table;

  private transient STEncoding stEncoding;

  private transient AvroPoint avroPoint;

  public String tableName;

  public HBaseTableSink(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
    configuration = HBaseConfiguration.create();
    connection = ConnectionFactory.createConnection(configuration);
    admin = connection.getAdmin();

    table = connection.getTable(TableName.valueOf(tableName));

    TimeDimensionDefinition timeDimDef = new TimePointDimensionDefinition(Calendar.HOUR);
    TimeEncoding timeEncoding = new ConcatenationTimeEncoding(timeDimDef);
    SFCDimensionDefinition[] dimensions = {
            new SFCDimensionDefinition(new BasicDimensionDefinition(-180.0, 180.0), 2),
            new SFCDimensionDefinition(new BasicDimensionDefinition(-90.0, 90.0), 2)};
    GridIndex hilbert = SFCFactory.createSpaceFillingCurve(dimensions, SFCFactory.SFCType.HILBERT);

    stEncoding = new ConcatenationEncoding(timeEncoding, hilbert);

    avroPoint = new AvroPoint();
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    System.out.println(value);

    LocalDateTime localDateTime = Instant.ofEpochMilli(value.getTimestamp())
            .atZone(ZoneOffset.ofHours(8)).toLocalDateTime();
    TimeValue timeValue = new TimeValue(
            localDateTime.getYear(),
            localDateTime.getMonthValue(),
            localDateTime.getDayOfMonth(),
            localDateTime.getHour(),
            localDateTime.getMinute(),
            localDateTime.getSecond());
    ByteArray index = stEncoding.getIndex(timeValue, new double[] {value.getLng(), value.getLat()});
    ByteArray rowKey = index.combine(new ByteArray(value.getId()));

    Put put = new Put(rowKey.getBytes());
    put.addColumn("f".getBytes(), "v".getBytes(), avroPoint.serialize(value));
    table.put(put);
  }

  @Override
  public void close() throws Exception {
    table.close();
    admin.close();
    connection.close();
  }
}
