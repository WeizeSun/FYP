package fyp;

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

public class StreamingKCluster {
  private int kTarget = 50;

  private static class Parser implements
    MapFunction<Element, Tuple3<Long, Element, Double>> {
      @Override
      public Tuple3<Long, Element, Double> map(Element elem) throws Exception {
        // 0 for ordinary element while positive for feedback centroids.
        return new Tuple3(0L, elem, 0.0);
      }
  }

  private static class ClassifyFunction
    extends RichFlatMapFunction<Tuple3<Long, Element, Double>, Tuple2<Long, Element>>
    implements CheckpointedFunction {

    private ListState<Tuple2<Long, Element>> centroidsState;
    private ListState<Double> fState;

    private Map<Long, Element> centroids = new HashMap<Long, Element>();
    private double f = -1.0;

    private final Random rand = new Random();

    @Override
    public void initializeState(FunctionInitializationContext context)
      throws Exception {
      ListStateDescriptor<Tuple2<Long, Element>> centroidsDescriptor
        = new ListStateDescriptor<>(
          "centroids",
          TypeInformation.of(new TypeHint<Tuple2<Long, Element>>() {})
        );
      centroidsState = context.getOperatorStateStore()
        .getListState(centroidsDescriptor);

      ListStateDescriptor<Double> fDescriptor
        = new ListStateDescriptor<>(
          "f",
          TypeInformation.of(new TypeHint<Double>() {})
        );
      fState = context.getOperatorStateStore()
        .getListState(fDescriptor);

      if (context.isRestored()) {
        for (Tuple2<Long, Element> entry: centroidsState.get()) {
          if (!centroids.containsKey(entry.f0)) {
            centroids.put(entry.f0, entry.f1);
          }
        }

        for (double ff: fState.get()) {
          f = ff;
          break;
        }
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context)
      throws Exception {
      centroidsState.clear();
      for (Map.Entry<Long, Element> entry: centroids.entrySet()) {
        centroidsState.add(new Tuple2(entry.getKey(), entry.getValue()));
      }

      fState.clear();
      fState.add(f);
    }

    @Override
    public void flatMap(Tuple3<Long, Element, Double> tuple,
      Collector<Tuple2<Long, Element>> out) throws Exception {
      Element element = tuple.f1;

      if (tuple.f0 > 0) {
        centroids.put(tuple.f0, element);
        f = tuple.f2;
        return;
      }

      if (f < 0) {
        // 0 for candidates while positive number for classified
        out.collect(new Tuple2(0L, element));
        return;
      }

      long cluster = -1;
      double minDistance = Double.POSITIVE_INFINITY;
      for (Map.Entry<Long, Element> entry: centroids.entrySet()) {
        double dist = element.distance(entry.getValue());
        if (dist < minDistance) {
          cluster = entry.getKey();
          minDistance = dist;
        }
      }
      if (Math.sqrt(Math.min(minDistance * minDistance / f, 1)) >= rand.nextDouble()) {
        out.collect(new Tuple2(0L, element));
      } else {
        out.collect(new Tuple2(cluster, element));
      }
    }
  }

  private static class ClassifiedFilter
    implements FilterFunction<Tuple2<Long, Element>> {
    @Override
    public boolean filter(Tuple2<Long, Element> tuple) throws Exception {
      return (tuple.f0 > 0);
    }
  }

  private static class CandidatesFilter
    implements FilterFunction<Tuple2<Long, Element>> {
    @Override
    public boolean filter(Tuple2<Long, Element> tuple) throws Exception {
      return (tuple.f0 == 0L);
    }
  }

  private static class CentroidsGenerator
    extends RichMapFunction<Tuple2<Long, Element>,
    Tuple3<Long, Element, Double>>
    implements CheckpointedFunction {
    private ListState<Double> fState;
    private ListState<Long> qState;
    private ListState<Element> centroidsState;

    private double f = 0.0;
    private long q = 0L;
    private List<Element> centroids = new LinkedList<Element>();

    private final Random rand = new Random();
    private final int k;
    private final int kTarget;
    private boolean initialized = false;

    public CentroidsGenerator(int kTarget) {
      this.kTarget = kTarget;
      this.k = (kTarget - 11) / 5;
    }

    @Override
    public void initializeState(FunctionInitializationContext context)
      throws Exception {
      ListStateDescriptor<Element> centroidsDescriptor
        = new ListStateDescriptor<>(
          "centroids",
          TypeInformation.of(new TypeHint<Element>() {})
        );
      centroidsState = context.getOperatorStateStore()
        .getListState(centroidsDescriptor);

      ListStateDescriptor<Double> fDescriptor
        = new ListStateDescriptor<>(
          "f",
          TypeInformation.of(new TypeHint<Double>() {})
        );
      fState = context.getOperatorStateStore()
        .getListState(fDescriptor);

      ListStateDescriptor<Long> qDescriptor
        = new ListStateDescriptor<>(
          "q",
          TypeInformation.of(new TypeHint<Long>() {})
        );
      qState = context.getOperatorStateStore()
        .getListState(qDescriptor);

      if (context.isRestored()) {
        for (Element entry: centroidsState.get()) {
          centroids.add(entry);
        }
        for (double ff: fState.get()) {
          f = ff;
          break;
        }
        for (long qq: qState.get()) {
          q = qq;
          break;
        }
      }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context)
      throws Exception {
      centroidsState.clear();
      qState.clear();
      for (Element entry: centroids) {
        centroidsState.add(entry);
      }

      fState.clear();
      fState.add(f);

      qState.add(q);
    }

    @Override
    public Tuple3<Long, Element, Double> map(Tuple2<Long, Element> tuple)
      throws Exception {
      Element element = tuple.f1;
      if (centroids.size() < k + 10) {
        centroids.add(element);
        return new Tuple3((long)centroids.size(), element, f);
      }
      if (!initialized && centroids.size() == k + 10) {
        initialized = true;
        PriorityQueue<Double> heap
          = new PriorityQueue<Double>(10, Collections.reverseOrder());
        LinkedList list = new LinkedList<Element>();
        for (Element elem: centroids) {
          list.add(elem);
        }
        Collection<Double> allValues = Element.allDistancesInSet(list);
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
        f = sum / 2;
      }
      long cluster = -1;
      long cur = 1;
      double minDistance = Double.POSITIVE_INFINITY;
      for (Element entry: centroids) {
        double dist = element.distance(entry);
        if (dist < minDistance) {
          cluster = cur;
          minDistance = dist;
        }
        cur++;
      }
      if (Math.sqrt(Math.min(minDistance * minDistance / f, 1)) >= rand.nextDouble()) {
          centroids.add(element);
          q += 1;
          if (q >= k) {
            q = 0;
            f *= 10;
          }
          return new Tuple3((long)centroids.size(), element, f);
      } else {
        return new Tuple3(cluster, element, -f);
      }
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
      return (tuple.f2 <= 0);
    }
  }

  public static class RealPointsToResult
    implements MapFunction<Tuple3<Long, Element, Double>,
    Tuple2<Long, Element>> {
    @Override
    public Tuple2<Long, Element> map(Tuple3<Long, Element, Double> tuple)
      throws Exception {
      return new Tuple2(tuple.f0, tuple.f1);
    }
  }

  public StreamingKCluster(int kTarget) {
    if (kTarget < 16) {
        System.out.println("Warning: the k value is too small, " +
                "will be automatically set to 16");
        kTarget = 16;
    }
    this.kTarget = kTarget;
  }

  public DataStream<Tuple2<Long, Element>> getStream(
    DataStream<Element> source) throws Exception {

    IterativeStream<Tuple3<Long, Element, Double>> parsed
      = source.map(new Parser()).iterate();

    DataStream<Tuple2<Long, Element>> firstRoundCheck
      = parsed.flatMap(new ClassifyFunction()).setParallelism(3);

    DataStream<Tuple2<Long, Element>> firstRoundElements
      = firstRoundCheck.filter(new ClassifiedFilter());

    DataStream<Tuple2<Long, Element>> firstRoundCentroids
      = firstRoundCheck.filter(new CandidatesFilter());

    DataStream<Tuple3<Long, Element, Double>> secondRoundCheck
      = firstRoundCentroids.map(new CentroidsGenerator(kTarget));

    DataStream<Tuple3<Long, Element, Double>> secondRoundElements
      = secondRoundCheck.filter(new RealPoints());

    DataStream<Tuple3<Long, Element, Double>> secondRoundCentroids
      = secondRoundCheck.filter(new NewCentroids()).broadcast();

    parsed.closeWith(secondRoundCentroids);

    DataStream<Tuple2<Long, Element>> result
      = secondRoundElements.map(new RealPointsToResult())
      .union(firstRoundElements);

    return result;
  }
}
