package fyp;

import org.apache.flink.metrics.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Random;
import java.util.PriorityQueue;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;

import java.io.IOException;

public class StreamingKmeans extends 
RichMapFunction<Tuple2<Integer, Element>, Integer> {

    private ListState<Element> centroids;
    private ValueState<Double> f;
    private ValueState<Long> count;
    private ValueState<Long> q;
    private MapState<Long, AtomicLong> filter;

    private final Random rand = new Random();
    private final int k;
    private final int kTarget;

    private class KMGauge implements Gauge<CentroidAndCount> {
        @Override
        public CentroidAndCount getValue() {
            try {
                return new CentroidAndCount(centroids.get()
                        , filter.entries());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new CentroidAndCount(new LinkedList<Element>(), 
                    new LinkedList<Entry<Long, AtomicLong>>());
        }
    }

    private KMGauge gauge;

    public Integer map(Tuple2<Integer, Element> tuple) {
        Element element = tuple.f1;
        long cur = 0;
        try {
            cur = count.value();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (cur == k + 9) {
            try {
                count.update(cur + 1L);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                filter.put(cur, new AtomicLong());
                centroids.add(element);
            } catch (Exception e) {
                e.printStackTrace();
            }

            PriorityQueue<Double> heap 
                = new PriorityQueue<Double>(10, Collections.reverseOrder());
            LinkedList<Element> elementsList = new LinkedList<Element>();
            try {
                for (Element ele: centroids.get()) {
                    elementsList.add(ele);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            Collection<Double> allValues = 
                Element.allDistancesInSet(elementsList);
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

            try {
                f.update(sum / 2);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return (int) cur + 1;
        } else if (cur < k + 9) {
            try {
                count.update(cur + 1L);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                centroids.add(element);
                filter.put(cur, new AtomicLong());
            } catch (Exception e) {
                e.printStackTrace();
            }
            return (int) cur + 1;
        } else {
            double p = Double.POSITIVE_INFINITY;
            long size = 1;
            try {
                for (Element temp: centroids.get()) {
                    size++;
                    double prob = temp.distance(element);
                    p = p < prob?p:prob;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            double curf = 0;
            try {
                curf = f.value();
            } catch (IOException e) {
                e.printStackTrace();
            }
            p = p * p / curf;
            p = p < 1?p:1;

            long curq = 0;
            if (rand.nextDouble() <= p) {
                try {
                    centroids.add(element);
                    filter.put(size, new AtomicLong());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    curq = q.value();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                try {
                    q.update(curq + 1L);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                curq = q.value();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (curq >= k) {
                try {
                    q.update(0L);
                    curf = f.value();
                    f.update(curf * 10);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            int classify = -1;
            double minDistance = Double.POSITIVE_INFINITY;
            try {
                int pos = 1;
                for (Element temp: centroids.get()) {
                    double dist = element.distance(temp);
                    if (dist < minDistance) {
                        classify = pos;
                        minDistance = dist;
                    }
                    pos++;
                }
                filter.get((long)pos).incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return classify;
        }
    }

    public void open(Configuration config) {
        this.gauge = getRuntimeContext()
            .getMetricGroup()
            .gauge("km", new KMGauge());

        this.centroids = getRuntimeContext()
            .getListState(new ListStateDescriptor<>
                    ("centroids", Element.class));

        MapStateDescriptor<Long, AtomicLong> filterDescriptor 
            = new MapStateDescriptor<>(
                    "filter",
                    TypeInformation.of(new TypeHint<Long>() {}),
                    TypeInformation.of(new TypeHint<AtomicLong>() {}));
        this.filter = getRuntimeContext().getMapState(filterDescriptor);

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
                    0.0);
        this.f = getRuntimeContext().getState(fDescriptor);

        ValueStateDescriptor<Long> qDescriptor 
            = new ValueStateDescriptor<>(
                    "q", 
                    TypeInformation.of(new TypeHint<Long>() {}),
                    0L);
        this.q = getRuntimeContext().getState(qDescriptor);
    }

    public StreamingKmeans(int k) {
        if (k < 16) {
            System.out.println("Warning: the k value is too small, " + 
                    "will be automatically set to 16");
            this.k = 16;
        } else {
            this.k = k;
        }
        this.kTarget = (k - 11) / 5;
    }
}
