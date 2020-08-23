package com.github.tm.glink.source;

import com.github.tm.glink.features.Point;

import java.io.IOException;

import java.util.LinkedList;


public class DidiGPSPointSource extends CSVGeoObjectSource<Point> {
  private final int servingSpeed;
  private final long dataStartTime;

  @Override
  public Point parseLine(String line) {
    String[] tokens = line.split(",");
    long dataEventTime = Long.parseLong(tokens[2]);
    return new Point(tokens[1], Double.parseDouble(tokens[3]), Double.parseDouble(tokens[4]), dataEventTime);
  }
  public DidiGPSPointSource(String dataFilePath, int servingSpeed, long dataStartTime) {
    this.filePath = dataFilePath;
    this.servingSpeed = servingSpeed;
    this.dataStartTime = dataStartTime;
  }

  public void run(SourceContext<Point> sourceContext) throws Exception {
    generateStream(sourceContext);
  }

  private void generateStream(SourceContext<Point> sourceContext) throws IOException, InterruptedException {
    long currentMaxTimeStamp = dataStartTime;
    long waitTime;
    long dataEventTime;
    String line;
    // buffer存储使最大时间戳发生变化两条记录的中间的记录。
    // 线程等待只会在当最大时间戳发生变化,且变化量大于10ms时发生。
    LinkedList<String> buffer = new LinkedList<>();
    while (bufferedReader.ready() && (line = bufferedReader.readLine()) != null) {
      String[] tokens = line.split(",");
      dataEventTime = Long.parseLong(tokens[2]);
      if (dataEventTime - currentMaxTimeStamp > 10L) {
        // 总等待时间为最大时间戳的变化值。通过除以加速值进行缩短。
        // 存储在buffer中的数据记录将会依次发出。
        waitTime = (dataEventTime - currentMaxTimeStamp) / servingSpeed;
        for (String str : buffer) {
          sourceContext.collect(parseLine(str));
        }
        Thread.sleep(waitTime);
        currentMaxTimeStamp = dataEventTime;
        buffer.clear();
      } else {
        buffer.add(line);
      }
      line = bufferedReader.readLine();
    }
    for (String str : buffer) {
      sourceContext.collect(parseLine(str));
    }
  }
}
