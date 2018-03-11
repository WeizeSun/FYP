package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;

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
    DataStream<Long> someIntegers = env.generateSequence(0, 1000);
    IterativeStream<Long> iteration = someIntegers.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        return value;
      }
    }).setParallelism(2).iterate();
    DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        return value - 1;
      }
    });
    DataStream<Long> minus1 = minusOne.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        return value;
      }
    });
    DataStream<Long> stillGreaterThanZero = minus1.filter(new FilterFunction<Long>() {
      @Override
      public boolean filter(Long value) throws Exception {
        return (value > 0);
      }
    }).setParallelism(2);
    iteration.closeWith(stillGreaterThanZero);
    DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
      @Override
      public boolean filter(Long value) throws Exception {
        return (value <= 0);
      }
    });
    lessThanZero.writeAsText("/home/weizesun/fuck.txt");
    env.execute("Iteration");
  }
}
