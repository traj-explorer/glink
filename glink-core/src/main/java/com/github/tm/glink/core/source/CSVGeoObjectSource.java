package com.github.tm.glink.core.source;

import com.github.tm.glink.features.GeoObject;
import com.github.tm.glink.features.TemporalObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

/**
 * @author Yu Liebing
 */
public abstract class CSVGeoObjectSource<T extends GeoObject> extends RichSourceFunction<T> {

  protected String filePath;
  protected BufferedReader bufferedReader;

  // Variables for speed up the simulated stream.
  private Integer speed_factor;
  private long startTime;
  private long startEventTime = -1;
  private long preEventTime;
  private int syncCounter;
  protected DateTimeFormatter formatter;

  public abstract T parseLine(String line);

  public CSVGeoObjectSource(String filePath) {
    this(filePath, 0);
  }

  public CSVGeoObjectSource(String filePath, int speed_factor) {
    this.filePath = filePath;
    this.speed_factor = speed_factor;
    this.startTime = Instant.now().toEpochMilli();
  }


  protected void checkTimeAndWait(T geoObject) throws InterruptedException {
    if (!(geoObject instanceof TemporalObject)) {
      return;
    }
    TemporalObject temporalObject = (TemporalObject) geoObject;
    long thisEventTime = temporalObject.getTimestamp();
    if(startEventTime<0) {
      startEventTime = thisEventTime;
      preEventTime = thisEventTime;
    } else {
      long gapTime = (thisEventTime - preEventTime)/speed_factor;
      if(gapTime>0 || syncCounter > 1000) {
        long currentTime = System.currentTimeMillis();
        long targetEmitTime = (long)((thisEventTime-startEventTime)/speed_factor) + startTime;
        long waitTime = targetEmitTime - currentTime;
        if (waitTime>0) {
          try {
            Thread.sleep(waitTime);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        syncCounter = 0;
      } else {
        syncCounter++;
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
