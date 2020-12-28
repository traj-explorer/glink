package com.github.tm.glink.connector.geomesa.util;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.locationtech.geomesa.utils.geotools.SchemaBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import javax.swing.event.ChangeEvent;

import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.Assert.*;

public class GeomesaTableSchemaTest {

  @Test
  public void test() {
    SchemaBuilder builder = SchemaBuilder.builder();
    builder.addInt("int");
    builder.add("t:Timestamp");
    SimpleFeatureType sft = builder.build("sft");

    SimpleFeatureBuilder sfb = new SimpleFeatureBuilder(sft);
    sfb.set("int", new GeomesaTableSchemaTest());
    sfb.set("t", 2);
    SimpleFeature sf = sfb.buildFeature("12");
    System.out.println(sf);
  }

}