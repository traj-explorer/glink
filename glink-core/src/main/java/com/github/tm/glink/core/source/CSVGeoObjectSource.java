package com.github.tm.glink.core.source;

import com.github.tm.glink.features.GeoObject;
import com.github.tm.glink.features.TemporalObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.format.DateTimeFormatter;

/**
 * @author Yu Liebing
 */
public abstract class CSVGeoObjectSource<T extends GeoObject> extends RichSourceFunction<T> {

  protected String filePath;
  protected BufferedReader bufferedReader;

  // Variables for speed up the simulative stream.
  protected Integer speed_factor;
  protected long curMaxTimestamp;
  protected DateTimeFormatter formatter;

  public abstract T parseLine(String line);

  public CSVGeoObjectSource() { }

  public CSVGeoObjectSource(String filePath) {
    this(filePath, 0);
  }

  public CSVGeoObjectSource(String filePath, int speed_factor) {
    this.filePath = filePath;
    this.speed_factor = speed_factor;
  }

  protected void checkTimeAndWait(T geoObject) throws InterruptedException {
    if (!(geoObject instanceof TemporalObject)) {
      return;
    }
    TemporalObject temporalObject = (TemporalObject) geoObject;
    if (curMaxTimestamp == 0) {
      curMaxTimestamp = temporalObject.getTimestamp();
      return;
    }
    long time2wait = temporalObject.getTimestamp() - curMaxTimestamp;
    // 10s触发一次等待
    if (time2wait > 10000) {
      synchronized (this){
        wait(time2wait/speed_factor);
        curMaxTimestamp = temporalObject.getTimestamp();
      }
    }
  }

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
      if (geoObject == null)
        continue;
      if (speed_factor > 0)
        checkTimeAndWait(geoObject);
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
