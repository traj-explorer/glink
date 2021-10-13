package com.github.tm.glink.core.areadetect;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

public abstract class AreaDetect<T> {

  StreamExecutionEnvironment env;
  TumblingEventTimeWindows windowAssigner;
  ProcessWindowFunction<DetectionUnit<T>, DetectionUnit<T>, Long, TimeWindow> interest;
  SingleOutputStreamOperator<DetectionUnit<T>> source;
  protected FlatMapFunction<DetectionUnit<T>, Tuple2<Long, DetectionUnit<T>>> router;
  protected LocalDetect<T> localDetectFunction;
  protected ProcessWindowFunction<Tuple2<Long, List<DetectionUnit<T>>>, Geometry, Long, TimeWindow> polygonGetFunc;



    public AreaDetect(StreamExecutionEnvironment env,
                    TumblingEventTimeWindows windowAssigner,
                    ProcessWindowFunction<DetectionUnit<T>, DetectionUnit<T>, Long, TimeWindow> interest,
                    SingleOutputStreamOperator<DetectionUnit<T>> source) {
    this.env = env;
    this.windowAssigner = windowAssigner;
    this.interest = interest;
    this.source = source;
  }

  public DataStream<Geometry> process() {
    final OutputTag<Tuple3<Long, DetectionUnit<T>, Long>> borderUnits = new OutputTag<Tuple3<Long, DetectionUnit<T>, Long>>("border") { };
    final OutputTag<Tuple2<Long, List<DetectionUnit<T>>>> needCombineTag = new OutputTag<Tuple2<Long, List<DetectionUnit<T>>>>("need combine") { };
    SingleOutputStreamOperator<Tuple2<Long, List<DetectionUnit<T>>>> localDetectStream = localDetect();
    DataStream<Tuple2<Long, Long>> fixRule = getFixRule(localDetectStream.getSideOutput(borderUnits));
    DataStream<Tuple2<Long, List<DetectionUnit<T>>>> globalAreas = assignGlobalID(fixRule, localDetectStream.getSideOutput(needCombineTag));
    return getPolygon(globalAreas, polygonGetFunc);
  }

  /**
   * 3路输出
   * @return
   */
  private SingleOutputStreamOperator<Tuple2<Long, List<DetectionUnit<T>>>> localDetect() {
    return source
        .keyBy(DetectionUnit::getId)
        .window(windowAssigner)
        .process(interest)
        .flatMap(router)
        .keyBy(f -> f.f0)
        .window(windowAssigner)
        .process(localDetectFunction);
  }

  private DataStream<Tuple2<Long, Long>> getFixRule(DataStream<Tuple3<Long, DetectionUnit<T>, Long>> marginUnitStream) {
    return marginUnitStream.keyBy(r -> r.f0).window(windowAssigner)
        .process(new LinkFinder<T>())
        .windowAll(windowAssigner)
        .process(new MapRuleGetter());
  }

  private DataStream<Tuple2<Long, List<DetectionUnit<T>>>> assignGlobalID(DataStream<Tuple2<Long, Long>> fixRuleStream, DataStream<Tuple2<Long, List<DetectionUnit<T>>>> toFixStream) {
    return fixRuleStream.keyBy(f -> f.f0).intervalJoin(toFixStream.keyBy(f -> f.f0))
        .between(Time.seconds(-1), Time.seconds(1))
        .process(new LocalFixFunction<T>());
  }

  private DataStream<Geometry> getPolygon(DataStream<Tuple2<Long, List<DetectionUnit<T>>>> partialAreas, ProcessWindowFunction<Tuple2<Long, List<DetectionUnit<T>>>, Geometry, Long, TimeWindow> getPolygonFunc) {
    return partialAreas
        .keyBy(f -> f.f0)
        .window(windowAssigner)
        .process(getPolygonFunc);
  }

  public abstract void initRouter();

  public abstract void initLocalDetectFunc();

  public abstract void initPolygonGetFunc();

}
