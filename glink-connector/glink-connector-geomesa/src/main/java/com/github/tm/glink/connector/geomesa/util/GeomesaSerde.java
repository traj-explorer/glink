package com.github.tm.glink.connector.geomesa.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/**
 * Helper class for serializing flink sql record to SimpleFeature.
 *
 * @author Yu Liebing
 * */
public class GeomesaSerde {

  private static final int MIN_TIMESTAMP_PRECISION = 0;
  private static final int MAX_TIMESTAMP_PRECISION = 3;
  private static final int MIN_TIME_PRECISION = 0;
  private static final int MAX_TIME_PRECISION = 3;

  private static final WKTReader WKTREADER = new WKTReader();
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  private static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

  @FunctionalInterface
  public interface GeomesaFieldEncoder extends Serializable {
    Object encode(RowData rowData, int pos);
  }

  public static GeomesaFieldEncoder getGeomesaFieldEncoder(LogicalType fieldType, boolean isSpatialField) {
    if (isSpatialField) {
      return ((rowData, pos) -> {
        try {
          return WKTREADER.read(rowData.getString(pos).toString());
        } catch (ParseException e) {
          throw new UnsupportedOperationException("Only support WKT string for spatial fields.");
        }
      });
    }
    // see GeomesaType for type mapping from flink sql to geomesa
    switch (fieldType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return ((rowData, pos) -> rowData.getString(pos).toString());
      case BOOLEAN:
        return (RowData::getBoolean);
      case BINARY:
      case VARBINARY:
        return (RowData::getBinary);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return RowData::getInt;
      case DATE:
        throw new UnsupportedOperationException("Currently not supported DATE, will be fix in the future");
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(fieldType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by "
                          + "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        throw new UnsupportedOperationException("Currently not supported TIME, will be fix in the future");
      case BIGINT:
      case INTERVAL_DAY_TIME:
        return RowData::getLong;
      case FLOAT:
        return RowData::getFloat;
      case DOUBLE:
        return RowData::getDouble;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final int timestampPrecision = getPrecision(fieldType);
        if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIMESTAMP type is out of the range [%s, %s] supported by "
                          + "HBase connector", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
        }
        return (row, pos) -> {
          long timestamp = row.getTimestamp(pos, timestampPrecision).getMillisecond();
          return new Timestamp(timestamp);
        };
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
    }
  }
}
