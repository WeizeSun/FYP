package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.checkpoint.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.runtime.state.*;

import java.util.Random;
import java.util.PriorityQueue;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;

import java.io.IOException;


public class Iteration {
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env
      = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> someLongs = env.generateSequence(0, 1000);
    DataStream<Tuple2<Long, Long>> result = someLongs.map(new MapFunction<Long, Tuple2<Long, Long>>() {
      @Override
      public Tuple2<Long, Long> map(Long value) throws Exception {
        return new Tuple2(1L, value);
      }
    }).map(new Parse()).setParallelism(3);
    result.writeAsText("/home/weizesun/fuck.txt");
    env.execute("Iteration");
  }
}

class Buffer
  extends RichMapFunction<Long, Long> implements CheckpointedFunction {
  private transient ListState<Long> state;
  private long outcome;
  public Long map(Long value) throws Exception {
    this.outcome = value * 2;
    return this.outcome;
  }
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    state.clear();
    state.add(outcome);
  }
  public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(
      "state",
      TypeInformation.of(new TypeHint<Long>() {})
    );
    state = context.getOperatorStateStore().getListState(descriptor);
    if (context.isRestored()) {
      for (long element: state.get()) {
        outcome = element;
        break;
      }
    }
  }
}

class Parse extends RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
  @Override
  public Tuple2<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
    return value;
  }
}
