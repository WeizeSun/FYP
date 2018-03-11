package fyp;

import org.apache.flink.metrics.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.*;

import java.util.*;
import java.util.Map.*;

import java.io.*;

public class StreamingK {
  private class ClassifyFunction extends
    RichMapFunction<Object, Tuple2<Element, Long>> {

    private MapState<Long, Element> centroids;
    private ValueState<Long> count;
    private ValueState<Double> f;

    private int k;
    private Random rand = new Random();

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
                  0L);
      this.count = getRuntimeContext().getState(countDescriptor);

      ValueStateDescriptor<Double> fDescriptor
          = new ValueStateDescriptor<>(
                  "f",
                  TypeInformation.of(new TypeHint<Double>() {}),
                  -1.0);
      this.f = getRuntimeContext().getState(fDescriptor);
    }

    @Override
    public Tuple2<Element, Long> map(Object object) throws Exception {
      long cnt = count.value();
      double ff = f.value();
      if (object instanceof Tuple4) {
        Tuple4<Integer, Long, Element, Double> tuple
          = (Tuple4<Integer, Long, Element, Double>) object;
        count.update(cnt + 1);
        centroids.put(tuple.f1, tuple.f2);
        f.update(tuple.f3);
        return new Tuple2(tuple.f2, 0);
      }

      Element element = (Element) object;
      if (ff < 0) {
        return new Tuple2(element, -1);
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
      if (Math.min(minDistance * minDistance / ff, 1) >= rand.nextDouble()) {
        return new Tuple2(element, -1);
      } else {
        return new Tuple2(element, cluster);
      }
    }

    public ClassifyFunction(int k) {
      this.k = k;
    }
  }

  private class ClassifiedFilter
    implements FilterFunction<Tuple2<Element, Long>> {
      @Override
      public boolean filter(Tuple2<Element, Long> tuple) throws Exception {
        return tuple.f1 > 0;
      }
  }

  private class UnclassifiedFilter
    implements FilterFunction<Tuple2<Element, Long>> {
      @Override
      public boolean filter(Tuple2<Element, Long> tuple) throws Exception {
        return tuple.f1 < 0;
      }
  }

  public StreamingK(int k) {}
}
