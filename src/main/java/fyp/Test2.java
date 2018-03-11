package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.*;

import java.util.Random;
import java.util.PriorityQueue;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;

import java.io.IOException;

public class Test2 {
  public static void main(String[] args)throws Exception {
    final StreamExecutionEnvironment env
      = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Long> someIntegers = env.generateSequence(0, 1000);
    DataStream<Tuple2<Integer, Long>> tuples = someIntegers.flatMap(new FlatMapFunction<Long, Tuple2<Integer, Long>>() {
      @Override
      public void flatMap(Long value, Collector<Tuple2<Integer, Long>> out) throws Exception {
        for (int i = 0; i < 3; i++) {
          out.collect(new Tuple2<Integer, Long>(i, value));
        }
      }
    });
    DataStream<Tuple2<Integer, Long>> output = tuples.map(new MapFunction<Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {
      @Override
      public Tuple2<Integer, Long> map(Tuple2<Integer, Long> tuple) throws Exception {
        return tuple;
      }
    }).setParallelism(3);
    DataStream<Long> result = output.map(new MapFunction<Tuple2<Integer, Long>, Long>() {
      @Override
      public Long map(Tuple2<Integer, Long> tuple) throws Exception {
        return tuple.f1;
      }
    });
    result.writeAsText("/home/weizesun/fuck.txt");
    env.execute("Test2");
  }
}
