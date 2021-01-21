package com.github.tm.glink.connector.geomesa.source;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class GeoMesaSourceFunction<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction {
  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

  }

  @Override
  public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

  }

  @Override
  public void run(SourceContext<T> sourceContext) throws Exception {
    System.out.println("+++");
  }

  @Override
  public void cancel() {

  }
}
