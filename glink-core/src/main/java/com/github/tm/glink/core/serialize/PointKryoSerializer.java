package com.github.tm.glink.core.serialize;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
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
    writeUserData(kryo, output, point);
  }

  @Override
  public Point read(Kryo kryo, Input input, Class aClass) {
    try {
      int geometryLen = input.readInt();
      byte[] data = new byte[geometryLen];
      input.readBytes(data);
      Point p = (Point) wkbReader.read(data);
      Object userData = readUserData(kryo, input);
      p.setUserData(userData);
      return p;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private void writeUserData(Kryo kryo, Output out, Point point) {
    out.writeBoolean(point.getUserData() != null);
    if (point.getUserData() != null) {
      kryo.writeClass(out, point.getUserData().getClass());
      kryo.writeObject(out, point.getUserData());
    }
  }

  private Object readUserData(Kryo kryo, Input input) {
    Object userData = null;
    if (input.readBoolean()) {
      Registration clazz = kryo.readClass(input);
      userData = kryo.readObject(input, clazz.getType());
    }
    return userData;
  }
}
