package com.github.tm.glink.core.areadetect;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * 计算DetectionUnit.val在时间窗口内的平均值，如果小于一个阈值，认为是兴趣/异常单元。
 */
public class InterestDetect extends ProcessWindowFunction<DetectionUnit<Double>, DetectionUnit<Double>, Long, TimeWindow> {

  private double threshold;

  public InterestDetect(double threshold){
    this.threshold = threshold;
  }

  @Override
  public void process(Long gridID, Context context, java.lang.Iterable<DetectionUnit<Double>> iterable, Collector<DetectionUnit<Double>> collector) throws Exception {
    int count = 0;
    double sum = 0;
    Iterator<DetectionUnit<Double>> iterator = iterable.iterator();
    while(iterator.hasNext()){
      count++;
      sum += iterator.next().getVal();
    }
    if(sum/count < threshold) {
      collector.collect(new DetectionUnit<Double>(gridID, context.window().getEnd(), sum / count));
    }
  }
}