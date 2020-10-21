package com.github.tm.glink.features.avro;

import com.github.tm.glink.features.GeoObject;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Yu Liebing
 * */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class AvroGeoObject<T extends GeoObject> implements Serializable {

  protected List<String> attributesName = new ArrayList<>();

  protected Schema schema;
  protected GenericRecord genericRecord;
  protected Injection<GenericRecord, byte[]> injection;

  public void init(String schema) {
    this.schema = new Schema.Parser().parse(schema);
    this.genericRecord = new GenericData.Record(this.schema);
    injection = GenericAvroCodecs.toBinary(this.schema);
  }

  public abstract byte[] serialize(T geoObject);

  public abstract T deserialize(byte[] data);

  protected void serializeAttributes(GenericRecord record, Properties attributes) {
    for (Map.Entry<Object, Object> e : attributes.entrySet()) {
      record.put((String) e.getKey(), e.getValue());
    }
  }

  protected Properties deserializeAttributes(GenericRecord record) {
    Properties attributes = new Properties();
    for (String name : attributesName) {
      attributes.put(name, record.get(name));
    }
    return attributes;
  }

  protected String toAvroSchema(String avroSchema, String attributesSchema) {
    String[] items = attributesSchema.split(";");
    String itemSchema = ",{\"name\": \"%s\", \"type\": [\"%s\", \"null\"]}";
    StringBuilder sb = new StringBuilder();
    for (String item : items) {
      String[] i = item.split(":");
      sb.append(String.format(itemSchema, i[0], i[1]));
      attributesName.add(i[0]);
    }
    return String.format(avroSchema, sb.toString());
  }
}
