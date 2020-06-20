package com.github.tm.glink.source;

import com.github.tm.glink.feature.Point;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * @author Yu Liebing
 */
public class CSVPointSource extends RichSourceFunction<Point> {

  private final String filePath;
  private BufferedReader bufferedReader;

  public CSVPointSource(String filePath) {
    this.filePath = filePath;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    FileReader fileReader = new FileReader(filePath);
    bufferedReader = new BufferedReader(fileReader);
  }

  @Override
  public void run(SourceContext<Point> sourceContext) throws Exception {
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] items = line.split(",");
      Point p = new Point(
              items[0],
              Float.parseFloat(items[1]),
              Float.parseFloat(items[2]),
              Long.parseLong(items[3]));
      sourceContext.collect(p);
    }
  }

  @Override
  public void cancel() {

  }

  @Override
  public void close() throws Exception {
    super.close();
    bufferedReader.close();
  }
}
