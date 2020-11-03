package com.github.tm.glink.format;

import com.github.tm.glink.enums.GeometryType;
import com.github.tm.glink.enums.TextFileSplitter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

/**
 * @author Yu Liebing
 */
public class TextFormatMap<T extends Geometry> extends RichFlatMapFunction<String, T> {

  protected transient GeometryFactory geometryFactory;
  protected transient WKTReader wktReader;
  protected transient WKBReader wkbReader;

  /**
   * The start offset.
   */
  protected final int startOffset;

  /**
   * The end offset. If the initial value is negative, Glink will consider each field as a spatial
   * attribute if the target object is LineString or Polygon.
   */
  protected final int endOffset;

  /**
   * The splitter.
   */
  protected final TextFileSplitter splitter;

  /**
   * The carry input data.
   */
  protected final boolean carryInputData;

  /**
   * Non-spatial attributes in each input row will be concatenated to a tab separated string
   */
  protected String otherAttributes = "";

  protected GeometryType geometryType = null;

  /**
   *  Allow mapping of invalid geometries.
   */
  protected boolean allowTopologicallyInvalidGeometries;

  /**
   *  Crash on syntactically invalid geometries or skip them.
   */
  protected boolean skipSyntacticallyInvalidGeometries;

  public TextFormatMap(
          int startOffset,
          int endOffset,
          TextFileSplitter splitter,
          boolean carryInputData,
          GeometryType geometryType) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
    this.splitter = splitter;
    this.carryInputData = carryInputData;
    this.geometryType = geometryType;
    allowTopologicallyInvalidGeometries = true;
    skipSyntacticallyInvalidGeometries = true;
    if (geometryType == null) {
      if (!splitter.equals(TextFileSplitter.WKB)
              && !splitter.equals(TextFileSplitter.WKT)
              && !splitter.equals(TextFileSplitter.GEOJSON)) {
        throw new IllegalArgumentException(
                "You must specify GeometryType when you use delimiter rather than WKB, WKT or GeoJSON");
      }
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    geometryFactory = new GeometryFactory();
    if (splitter.equals(TextFileSplitter.WKT)) {
      wktReader = new WKTReader();
    }
    if (splitter.equals(TextFileSplitter.WKB)) {
      wkbReader = new WKBReader();
    }
  }

  @Override
  public void flatMap(String line, Collector<T> collector) throws Exception {
    T geometry = (T) readGeometry(line);
    collector.collect(geometry);
  }

  private Geometry readGeometry(String line) {
    Geometry geometry = null;
    try {
      switch (splitter) {
        case WKT:
          // TODO
          break;
        case WKB:
          // TODO
          break;
        case GEOJSON:
          // TODO
          break;
        default:
          geometry = createGeometry(readCoordinates(line), geometryType);
          break;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return geometry;
  }

  private Geometry createGeometry(Coordinate[] coordinates, GeometryType geometryType) {
    Geometry geometry;
    switch (geometryType) {
      case POLYGON:
        geometry = geometryFactory.createPolygon(coordinates);
        break;
      case LINESTRING:
        geometry = geometryFactory.createLineString(coordinates);
        break;
      case RECTANGLE:
        // The rectangle mapper reads two coordinates from the input line. The two coordinates are the two on the diagonal.
        assert  coordinates.length == 2;
        Coordinate[] polyCoordinates = new Coordinate[5];
        polyCoordinates[0] = coordinates[0];
        polyCoordinates[1] = new Coordinate(coordinates[0].x, coordinates[1].y);
        polyCoordinates[2] = coordinates[1];
        polyCoordinates[3] = new Coordinate(coordinates[1].x, coordinates[0].y);
        polyCoordinates[4] = polyCoordinates[0];
        geometry = geometryFactory.createPolygon(polyCoordinates);
        break;
      // Read string to point if no geometry type specified but GeoSpark should never reach here
      default:
        geometry = geometryFactory.createPoint(coordinates[0]);
    }
    if (carryInputData) {
      geometry.setUserData(otherAttributes);
    }
    return geometry;
  }

  private Coordinate[] readCoordinates(String line) {
    final String[] columns = line.split(splitter.getDelimiter());
    final int actualEndOffset = this.endOffset >= 0
            ? this.endOffset : (this.geometryType == GeometryType.POINT ? startOffset + 1 : columns.length - 1);
    final Coordinate[] coordinates = new Coordinate[(actualEndOffset - startOffset + 1) / 2];
    for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
      coordinates[(i - startOffset) / 2 ] = new Coordinate(Double.parseDouble(columns[i]), Double.parseDouble(columns[i + 1]));
    }
    if (carryInputData) {
      boolean firstColumnFlag = true;
      otherAttributes = "";
      for (int i = 0; i < startOffset; i++) {
        if (firstColumnFlag) {
          otherAttributes += columns[i];
          firstColumnFlag = false;
        } else otherAttributes += "\t" + columns[i];
      }
      for (int i = actualEndOffset + 1; i < columns.length; i++) {
        if (firstColumnFlag) {
          otherAttributes += columns[i];
          firstColumnFlag = false;
        } else otherAttributes += "\t" + columns[i];
      }
    }
    return coordinates;
  }
}
