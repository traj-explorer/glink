package com.github.tm.glink.connector.geomesa.util;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.Serializable;

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

  private static final WKTReader wktReader = new WKTReader();

  @FunctionalInterface
  public interface GeomesaFieldEncoder extends Serializable {
    Object encode(RowData rowData, int pos);
  }

  public static GeomesaFieldEncoder getGeomesaFieldEncoder(LogicalType fieldType, boolean isSpatialField) {
    if (isSpatialField) {
      return ((rowData, pos) -> {
        try {
          return wktReader.read(rowData.getString(pos).toString());
        } catch (ParseException e) {
          throw new UnsupportedOperationException("Only support WKT string");
        }
      });
    }
    // ordered by type root definition
    switch (fieldType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        // get the underlying UTF-8 bytes
        return ((rowData, pos) -> rowData.getString(pos).toString());
      case BOOLEAN:
        return (RowData::getBoolean);
      case BINARY:
      case VARBINARY:
        return (RowData::getBinary);
      case DECIMAL:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case DATE:
      case INTERVAL_YEAR_MONTH:
        return RowData::getInt;
      case TIME_WITHOUT_TIME_ZONE:
        final int timePrecision = getPrecision(fieldType);
        if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
          throw new UnsupportedOperationException(
                  String.format("The precision %s of TIME type is out of the range [%s, %s] supported by "
                          + "HBase connector", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
        }
        return RowData::getInt;
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
        return (row, pos) -> row.getTimestamp(pos, timestampPrecision).getMillisecond();
      default:
        throw new UnsupportedOperationException("Unsupported type: " + fieldType);
    }
  }
}
