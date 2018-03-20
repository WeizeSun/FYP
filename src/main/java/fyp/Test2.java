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
    DataStream<Long> someIntegers = env.generateSequence(0, 100000);
    DataStream<Long> tuples = someIntegers.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long value) throws Exception {
        return value;
      }
    }).broadcast();
    DataStream<Long> output = tuples.map(new MapFunction<Long, Long>() {
      @Override
      public Long map(Long tuple) throws Exception {
        return tuple;
      }
    }).setParallelism(3);
    output.writeAsText("/home/weizesun/fuck.txt");
    env.execute("Test2");
  }
}
