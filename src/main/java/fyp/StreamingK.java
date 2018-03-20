package fyp;

import org.apache.flink.metrics.*;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.checkpoint.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.*;
import org.apache.flink.util.*;
import org.apache.flink.runtime.state.*;

import java.util.*;
import java.util.Map.*;

import java.io.*;

public class StreamingK {
  private int kTarget = 30;

  private static class ClassifyFunction extends
    RichMapFunction<Object, Tuple2<Element, Long>> implements
    CheckpointedFunction {

    private MapState<Long, Element> centroidsState;
    private ValueState<Long> countState;
    private ValueState<Double> fState;

    private Map centroids = new HashMap<Long, Element>();
    private long count = 0;
    private double f = -1.0;

    private Random rand = new Random();

    @Override
    public void initializeState(FunctionInitializationContext context)
      throws Exception {
      MapStateDescriptor<Long, Element> centroidsDescriptor
        = new MapStateDescriptor<>(
          "centroids",
          TypeInformation.of(new TypeHint<Long>() {}),
          TypeInformation.of(new TypeHint<Element>() {})
      );
      centroidsState = context.getKeyedStateStore()
        .getMapState(centroidsDescriptor);

      ValueStateDescriptor<Long> countDescriptor
        = new ValueStateDescriptor<>(
        "count",
        TypeInformation.of(new TypeHint<Long>() {}),
        0L
      );
      countState = context.getKeyedStateStore()
        .getState(countDescriptor);

      ValueStateDescriptor<Double> fDescriptor
        = new ValueStateDescriptor<>(
        "f",
        TypeInformation.of(new TypeHint<Double>() {}),
        -1.0
      );
      fState = context.getKeyedStateStore().getState(fDescriptor);

      if (context.isRestored()) {
        for (Map.Entry<Long, Element> entry: centroidsState.entries()) {
          if (!centroids.containsKey(entry.getKey())) {
            centroids.put(entry.getKey(), entry.getValue());
          }
        }

        count = countState.value();
        f = fState.value();
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context)
      throws Exception {
      centroidsState.clear();
      for (Map.Entry<Long, Element> entry:
        (Set<Map.Entry<Long, Element>>)centroids.entrySet()) {
        if (!centroidsState.contains(entry.getKey())) {
          centroidsState.put(entry.getKey(), entry.getValue());
        }
      }
      countState.update(count);
      fState.update(f);
    }

    @Override
    public Tuple2<Element, Long> map(Object object) throws Exception {
      if (object instanceof Tuple4) {
        Tuple4<Integer, Long, Element, Double> tuple
          = (Tuple4<Integer, Long, Element, Double>) object;
        centroids.put(tuple.f1, tuple.f2);
        count++;
        f = tuple.f3;
        return new Tuple2(tuple.f2, 0);
      }

      Element element = (Element) object;
      if (f < 0) {
        return new Tuple2(element, -1);
      }

      long cluster = -1;
      double minDistance = Double.POSITIVE_INFINITY;
      for (Map.Entry<Long, Element> entry:
        (Set<Map.Entry<Long, Element>>)centroids.entrySet()) {
        double dist = element.distance(entry.getValue());
        if (dist < minDistance) {
          cluster = entry.getKey();
          minDistance = dist;
        }
      }
      if (Math.min(minDistance * minDistance / f, 1) >= rand.nextDouble()) {
        return new Tuple2(element, -1);
      } else {
        return new Tuple2(element, cluster);
      }
    }
  }

  private static class ClassifiedFilter
    implements FilterFunction<Tuple2<Element, Long>> {
    @Override
    public boolean filter(Tuple2<Element, Long> tuple) throws Exception {
      return tuple.f1 > 0;
    }
  }

  private static class UnclassifiedFilter
    implements FilterFunction<Tuple2<Element, Long>> {
      @Override
      public boolean filter(Tuple2<Element, Long> tuple) throws Exception {
        return tuple.f1 < 0;
      }
  }

  private static class CentroidsGenerator
    extends RichMapFunction<Tuple2<Element, Long>,
      Tuple3<Long, Element, Double>> {

      private ValueState<Double> f;
      private ValueState<Long> count;
      private ValueState<Long> q;
      private MapState<Long, Element> centroids;

      private final Random rand = new Random();
      private final int k;
      private final int kTarget;
      private boolean initialized = false;

      public Tuple3<Long, Element, Double> map(Tuple2<Element, Long> tuple)
        throws Exception {
        long cnt = count.value();
        Element element = tuple.f0;
        double ff = f.value();
        if (cnt < k + 10) {
          count.update(cnt + 1);
          centroids.put(cnt + 1, element);
          return new Tuple3(cnt + 1, element, ff);
        }
        if (!initialized && cnt == k + 10) {
          initialized = true;
          PriorityQueue<Double> heap
            = new PriorityQueue<Double>(10, Collections.reverseOrder());
          LinkedList list = new LinkedList<Element>();
          for (Element elem: centroids.values()) {
            list.add(elem);
          }
          Collection<Double> allValues
            = Element.allDistancesInSet(list);
          for (double temp: allValues) {
            heap.add(temp);
            if (heap.size() > 10) {
              heap.poll();
            }
          }
          double sum = 0;
          for (double temp: heap) {
            sum += temp * temp;
          }
          f.update(sum / 2);
        }
        long cluster = -1;
        double minDistance = Double.POSITIVE_INFINITY;
        for (Map.Entry<Long, Element> entry: centroids.entries()) {
          double dist = element.distance(entry.getValue());
          if (dist < minDistance) {
            cluster = entry.getKey();
            minDistance = dist;
          }
        }
        if (Math.min(minDistance * minDistance / ff, 1)
          >= rand.nextDouble()) {
            centroids.put(cnt + 1, element);
            long qq = q.value();
            if (qq + 1 >= k) {
              q.update(0L);
              f.update(ff * 10);
            } else {
              q.update(qq + 1);
            }
            return new Tuple3(cnt + 1, element, f.value());
        } else {
          return new Tuple3(cluster, element, -ff);
        }
      }

      public void open(Configuration config) {
        MapStateDescriptor<Long, Element> centroidsDescriptor
          = new MapStateDescriptor<>(
          "centroids",
          TypeInformation.of(new TypeHint<Long>() {}),
          TypeInformation.of(new TypeHint<Element>() {})
        );
        this.centroids = getRuntimeContext().getMapState(centroidsDescriptor);

        ValueStateDescriptor<Long> countDescriptor
          = new ValueStateDescriptor<>(
          "count",
          TypeInformation.of(new TypeHint<Long>() {}),
          0L
        );
        this.count = getRuntimeContext().getState(countDescriptor);

        ValueStateDescriptor<Double> fDescriptor
          = new ValueStateDescriptor<>(
          "f",
          TypeInformation.of(new TypeHint<Double>() {}),
          0.0
        );
        this.f = getRuntimeContext().getState(fDescriptor);

        ValueStateDescriptor<Long> qDescriptor
          = new ValueStateDescriptor<>(
          "q",
          TypeInformation.of(new TypeHint<Long>() {}),
          0L
        );
        this.q = getRuntimeContext().getState(qDescriptor);
      }

      public CentroidsGenerator(int kTarget) {
        this.kTarget = kTarget;
        this.k = (kTarget - 11) / 5;
      }
  }

  public static class NewCentroids
    implements FilterFunction<Tuple3<Long, Element, Double>> {
    @Override
    public boolean filter(Tuple3<Long, Element, Double> tuple)
      throws Exception {
        return tuple.f2 > 0;
    }
  }

  public static class RealPoints
    implements FilterFunction<Tuple3<Long, Element, Double>> {
    @Override
    public boolean filter(Tuple3<Long, Element, Double> tuple)
      throws Exception {
        return tuple.f2 < 0;
    }
  }

  public static class RealPointsToResult
    implements MapFunction<Tuple3<Long, Element, Double>,
      Tuple2<Element, Long>> {
    @Override
    public Tuple2<Element, Long> map(Tuple3<Long, Element, Double> tuple)
      throws Exception {
      return new Tuple2(tuple.f1, tuple.f0);
    }
  }

  public static class CentroidsDistributor
    implements FlatMapFunction<Tuple3<Long, Element, Double>, Object> {
    private final int parallelism;

    @Override
    public void flatMap(Tuple3<Long, Element, Double> tuple,
      Collector<Object> out)
        throws Exception {
      for (int i = 0; i < parallelism; i++) {
        out.collect(new Tuple4(i, tuple.f0, tuple.f1, tuple.f2));
      }
    }

    public CentroidsDistributor(int parallelism) {
      this.parallelism = parallelism;
    }
  }

  public StreamingK(int kTarget) {
    if (kTarget < 16) {
        System.out.println("Warning: the k value is too small, " +
                "will be automatically set to 16");
        kTarget = 16;
    }
    this.kTarget = kTarget;
  }

  public static void main(String[] args) throws Exception {
    String inputPath = args[0], outputPath = args[1];
    int parallelism = 1, kTarget = 30;
    final StreamExecutionEnvironment env
      = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> source = env.readTextFile(inputPath);

    IterativeStream<Object> parsed
      = source.map(new MapFunction<String, Object>() {
      @Override
      public Object map(String value) throws Exception {
        String[] stringArray = value.split(",");
        double[] doubleArray = new double[stringArray.length];
        for (int i = 0; i < stringArray.length; i++) {
          doubleArray[i] = Double.parseDouble(stringArray[i]);
        }
        return new Element(doubleArray);
      }
    }).iterate();

    DataStream<Tuple2<Element, Long>> firstRoundCheck
      = parsed.map(new ClassifyFunction())
      .setParallelism(parallelism);

    DataStream<Tuple2<Element, Long>> firstRoundElements
      = firstRoundCheck.filter(new ClassifiedFilter());

    DataStream<Tuple2<Element, Long>> firstRoundCentroids
      = firstRoundCheck.filter(new UnclassifiedFilter());

    DataStream<Tuple3<Long, Element, Double>> secondRoundCheck
      = firstRoundCentroids.map(new CentroidsGenerator(kTarget));

    DataStream<Tuple3<Long, Element, Double>> secondRoundElements
      = secondRoundCheck.filter(new RealPoints());

    DataStream<Tuple3<Long, Element, Double>> secondRoundCentroids
      = secondRoundCheck.filter(new NewCentroids());

    DataStream<Object> distributedCentroids
      = secondRoundCentroids.flatMap(new CentroidsDistributor(parallelism))
      .setParallelism(parallelism);

    parsed.closeWith(distributedCentroids);

    DataStream<Tuple2<Element, Long>> result
      = secondRoundElements.map(new RealPointsToResult())
      .union(firstRoundElements);

    result.writeAsText(outputPath);
    env.execute("StreamingK");
  }
}
