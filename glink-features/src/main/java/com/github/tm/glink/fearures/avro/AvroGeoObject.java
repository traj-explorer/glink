package com.github.tm.glink.fearures.avro;

import com.github.tm.glink.fearures.GeoObject;

/**
 * @author Yu Liebing
 * */
public abstract class AvroGeoObject<T extends GeoObject> {

  public abstract byte[] serialize(T geoObject);

  public abstract T deserialize(byte[] data);
}
