package com.github.tm.glink.core.source;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.locationtech.jts.geom.Geometry;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public abstract class CSVGeometrySource<T extends Geometry> extends RichSourceFunction<T> {

  protected String filePath;
  protected BufferedReader bufferedReader;

  // Variables for speed up the simulated stream.
  private Integer speedFactor;
  private long startTime;
  private long startEventTime = -1;
  private long preEventTime;
  private int syncCounter;
  private int timestampOffsetInUserData;
  private DateTimeFormatter formatter;

  public abstract T parseLine(String line);

  public CSVGeometrySource(String filePath, int speedFactor, int timestampOffsetInUserData, DateTimeFormatter formatter) {
    this.filePath = filePath;
    this.speedFactor = speedFactor;
    this.timestampOffsetInUserData = timestampOffsetInUserData;
    this.formatter = formatter;
    startTime = Instant.now().toEpochMilli();
  }


  protected void checkTimeAndWait(T geoObject) throws InterruptedException {
    Tuple userData = (Tuple) ((Geometry) geoObject).getUserData();
    long thisEventTime = userData.getField(timestampOffsetInUserData);
    if (startEventTime < 0) {
      startEventTime = thisEventTime;
      preEventTime = thisEventTime;
    } else {
      long gapTime = (thisEventTime - preEventTime) / speedFactor;
      if (gapTime > 0 || syncCounter > 1000) {
        long currentTime = System.currentTimeMillis();
        long targetEmitTime = (long) ((thisEventTime - startEventTime) / speedFactor) + startTime;
        long waitTime = targetEmitTime - currentTime;
        if (waitTime > 0) {
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
      if (speedFactor > 0)
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
