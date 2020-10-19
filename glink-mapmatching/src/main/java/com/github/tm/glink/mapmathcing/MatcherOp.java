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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * @author Yu Liebing
 */
public class MatcherOp<T extends TrajectoryPoint>
        extends RichMapFunction<T, TrajectoryPoint> {

  private transient Matcher matcher;
  private transient ValueState<MatcherKState> state;

  @Override
  public void open(Configuration parameters) throws Exception {
    ValueStateDescriptor<MatcherKState> descriptor = new ValueStateDescriptor<>(
            "state", TypeInformation.of(new TypeHint<MatcherKState>() {}), new MatcherKState());
    state = getRuntimeContext().getState(descriptor);

    ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    RoadMap map = Loader.roadmap(parameterTool.get("road-properties-path"), false).construct();
    // Instantiate matcher and state data structure
    matcher = new Matcher(map, new Dijkstra<>(), new TimePriority(), new Geography());
  }

  @Override
  public TrajectoryPoint map(T p) throws Exception {
    System.out.println(Thread.currentThread().getId());
    System.out.println(p);
    MatcherKState currentKState = state.value();
    Point point = new Point(p.getLng(), p.getLat());
    MatcherSample sample = new MatcherSample(String.valueOf(p.getPid()), p.getTimestamp(), point);
    currentKState.update(matcher.execute(currentKState.vector(), currentKState.sample(), sample), sample);
    state.update(currentKState);
    MatcherCandidate candidate = currentKState.estimate();

    if (candidate == null) return new TrajectoryPoint();
    Point roadPoint = candidate.point().geometry();
    return new TrajectoryPoint(p.getId(), p.getPid(), roadPoint.getY(), roadPoint.getX(), p.getTimestamp());
  }
}
