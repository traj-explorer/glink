package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.GeoObject;

import java.io.Serializable;

/**
 * @author Yu Liebing
 * */
public abstract class AvroGeoObject<T extends GeoObject> implements Serializable {

  public abstract byte[] serialize(T geoObject);

  public abstract T deserialize(byte[] data);
}
