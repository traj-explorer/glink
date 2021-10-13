package com.github.tm.glink.core.areadetect;

import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;

public class ExampleDataSource implements SourceFunction<DetectionUnit<Double>> {

  private String filePath;
  private GeographicalGridIndex geographicalGridIndex;


  public ExampleDataSource(String filePath, GeographicalGridIndex geographicalGridIndex) throws FileNotFoundException {
    this.filePath = filePath;
    this.geographicalGridIndex = geographicalGridIndex;
  }

  @Override
  public void run(SourceContext<DetectionUnit<Double>> sourceContext) throws Exception {
    BufferedReader br  = new BufferedReader(new FileReader(filePath));
    while(br.ready()){
      String line = br.readLine();
      String[] vals = line.split(" ");
      double lat = Double.parseDouble(vals[1]);
      double lng = Double.parseDouble(vals[2]);
      Long id = geographicalGridIndex.getIndex(lat, lng);
      DetectionUnit<Double> unigridUnit = new DetectionUnit<>(id, Long.parseLong(vals[3]), Double.parseDouble(vals[4]));
      sourceContext.collect(unigridUnit);
    }
  }

  @Override
  public void cancel() {
  }
}
