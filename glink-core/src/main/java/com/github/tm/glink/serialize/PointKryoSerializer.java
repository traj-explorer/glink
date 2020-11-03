package com.github.tm.glink.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

/**
 * @author Yu Liebing
 */
public class PointKryoSerializer extends JavaSerializer<Point> {

  private final WKBReader wkbReader = new WKBReader();
  private final WKBWriter wkbWriter = new WKBWriter();

  @Override
  public void write(Kryo kryo, Output output, Point point) {
    byte[] geometryData = wkbWriter.write(point);
    output.writeInt(geometryData.length);
    output.write(geometryData);
    if (point.getUserData() == null) {
      output.writeString("");
    } else {
      String userData = point.getUserData().toString();
      output.writeString(userData);
    }
  }

  @Override
  public Point read(Kryo kryo, Input input, Class aClass) {
    try {
      int geometryLen = input.readInt();
      byte[] data = new byte[geometryLen];
      input.readBytes(data);
      Point p = (Point) wkbReader.read(data);
      String userData = input.readString();
      if (!userData.equals("")) {
        p.setUserData(userData);
      }
      return p;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}
