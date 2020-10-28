package com.github.tm.glink.mapmathcing;

import com.bmwcarit.barefoot.matcher.Matcher;
import com.bmwcarit.barefoot.matcher.MatcherCandidate;
import com.bmwcarit.barefoot.matcher.MatcherKState;
import com.bmwcarit.barefoot.matcher.MatcherSample;
import com.bmwcarit.barefoot.roadmap.*;
import com.bmwcarit.barefoot.spatial.Geography;
import com.bmwcarit.barefoot.topology.Dijkstra;
import com.esri.core.geometry.Point;
import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author Yu Liebing
 */
public class MatcherOp<T extends TrajectoryPoint>
        extends RichFlatMapFunction<T, TrajectoryPoint> {

  private transient RoadMap map;
  private transient Matcher matcher;
  private transient ValueState<MatcherKState> state;

  @Override
  public void open(Configuration parameters) throws Exception {
    StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();
    ValueStateDescriptor<MatcherKState> descriptor = new ValueStateDescriptor<>(
            "matcher-k-state", TypeInformation.of(new TypeHint<MatcherKState>() { }), new MatcherKState());
//    descriptor.enableTimeToLive(ttlConfig);
    state = getRuntimeContext().getState(descriptor);

    ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    map = Loader.roadmap(parameterTool.get("road-properties-path"), false).construct();
    // Instantiate matcher and state data structure
    matcher = new Matcher(map, new Dijkstra<>(), new TimePriority(), new Geography());
  }

  @Override
  public void flatMap(T p, Collector<TrajectoryPoint> collector) throws Exception {
    // get k state
    MatcherKState currentKState = state.value();
    // create matcher sample
    Point point = new Point(p.getLng(), p.getLat());
    MatcherSample sample = new MatcherSample(String.valueOf(p.getPid()), p.getTimestamp(), point);
    // do match
    currentKState.update(matcher.execute(currentKState.vector(), currentKState.sample(), sample), sample);
    // update k state
    state.update(currentKState);
    // get match result
    MatcherCandidate candidate = currentKState.estimate();
    if (candidate == null) return;
    Point roadPoint = candidate.point().geometry();
    long roadId = candidate.point().edge().base().refid();
    Properties attributes = new Properties();
    attributes.put("roadLat", roadPoint.getY());
    attributes.put("roadLng", roadPoint.getX());
    attributes.put("roadId", roadId);
    p.setAttributes(attributes);
    System.out.println(p + ", roadId: " + roadId);
    collector.collect(p);
  }

  @Override
  public void close() throws Exception {
    map.deconstruct();
  }
}
