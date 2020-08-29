package com.github.tm.glink.source;

import com.github.tm.glink.features.GeoObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author Yu Liebing
 */
public abstract class CSVGeoObjectSource<T extends GeoObject> extends RichSourceFunction<T> {

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected String filePath;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected BufferedReader bufferedReader;

  public abstract T parseLine(String line);

  @Override
  public void open(Configuration parameters) throws Exception {
    FileReader fileReader = new FileReader(filePath);
    bufferedReader = new BufferedReader(fileReader);
  }

  @Override
  public final void run(SourceContext<T> sourceContext) throws Exception {
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      T geoObject = parseLine(line);
      sourceContext.collect(geoObject);
    }
  }

  @Override
  public final void cancel() {

  }

  @Override
  public final void close() throws Exception {
    bufferedReader.close();
  }
}
