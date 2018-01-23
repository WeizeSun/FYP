package fyp;

import org.apache.flink.metrics.*;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Random;
import java.util.PriorityQueue;
import java.util.LinkedList;
import java.util.Collection;
import java.util.Collections;

import java.io.IOException;

public class StreamingKmeans extends 
RichMapFunction<Tuple2<Integer, Element>, Integer> {

    private ListState<Element> centroids;
    private ValueState<Double> f;
    private ValueState<Long> count;
    private ValueState<Long> q;

    private final Random rand = new Random();
    private final int k;
    private final int kTarget;

    private class KMGauge implements Gauge<Iterable<Element>> {
        @Override
        public Iterable<Element> getValue() {
            try {
                return centroids.get();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception occurred in gauge!");
            }
            return new LinkedList<Element>();
        }
    }

    private KMGauge gauge;

    public Integer map(Tuple2<Integer, Element> tuple) {
        Element element = tuple.f1;
        long cur = 0;
        try {
            cur = count.value();
        } catch (IOException e) {
            System.out.println("Caught IOException in " 
                    + "StreamingKmeans.map(), " 
                    + "when doing ValueState.value()");
        }

        if (cur == k + 9) {
            try {
                count.update(cur + 1L);
            } catch (IOException e) {
                System.out.println("Caught IOException in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.update");
            }

            try {
                centroids.add(element);
            } catch (Exception e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ListState.add()");
            }

            PriorityQueue<Double> heap 
                = new PriorityQueue<Double>(10, Collections.reverseOrder());
            LinkedList<Element> elementsList = new LinkedList<Element>();
            try {
                for (Element ele: centroids.get()) {
                    elementsList.add(ele);
                }
            } catch (IOException e) {
                System.out.println("Caught IOException in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ListState.add()");
            } catch (Exception e) {
                System.out.println("Caught IOException in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ListState.add()");
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
                System.out.println("Caught IOException in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.update()");
            }
            return (int) cur + 1;
        } else if (cur < k + 9) {
            try {
                count.update(cur + 1L);
            } catch (IOException e) {
                System.out.println("Caught IOException in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.update");
            }

            try {
                centroids.add(element);
            } catch (Exception e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ListState.add()");
            }
            return (int) cur + 1;
        } else {
            double p = Double.POSITIVE_INFINITY;

            try {
                for (Element temp: centroids.get()) {
                    double prob = temp.distance(element);
                    p = p < prob?p:prob;
                }
            } catch (Exception e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ListState.get()");
            }

            double curf = 0;
            try {
                curf = f.value();
            } catch (IOException e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.value()");
            }
            p = p * p / curf;
            p = p < 1?p:1;

            long curq = 0;
            if (rand.nextDouble() <= p) {
                try {
                    centroids.add(element);
                } catch (Exception e) {
                    System.out.println("Caught Exception in " 
                            + "StreamingKmeans.map(), " 
                            + "when doing ListState.add()");
                }
                try {
                    curq = q.value();
                } catch (IOException e) {
                    System.out.println("Caught Exception in " 
                            + "StreamingKmeans.map(), " 
                            + "when doing ValueState.value()");
                }

                try {
                    q.update(curq + 1L);
                } catch (IOException e) {
                    System.out.println("Caught Exception in " 
                            + "StreamingKmeans.map(), " 
                            + "when doing ValueState.update()");
                }
            }

            try {
                curq = q.value();
            } catch (IOException e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.value()");
            }

            if (curq >= k) {
                try {
                    q.update(0L);
                    curf = f.value();
                    f.update(curf * 10);
                } catch (IOException e) {
                    System.out.println("Caught Exception in " 
                            + "StreamingKmeans.map(), " 
                            + "when doing ValueState.update()");
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
            } catch (Exception e) {
                System.out.println("Caught Exception in " 
                        + "StreamingKmeans.map(), " 
                        + "when doing ValueState.update()");
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
