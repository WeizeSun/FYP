package fyp;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map.Entry;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Scanner;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.concurrent.atomic.AtomicLong;

public class Log4JReporter implements MetricReporter, Scheduled {
    private final static Logger log 
        = LoggerFactory.getLogger(Log4JReporter.class);
    private final HashMap<Gauge<?>, String> gauges 
        = new HashMap();

    @Override
    public void open(MetricConfig metricconfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, 
            String metricName, MetricGroup group) {
        String name = group.getMetricIdentifier(metricName);

        synchronized(this) {
            if (metric instanceof Gauge<?>) {
                gauges.put((Gauge<?>)metric, name);
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric,
            String metricName, MetricGroup group) {
        synchronized(this) {
            if (metric instanceof Gauge<?>) {
                gauges.remove(metric);
            }
        }
    }

    @Override
    public void report() {
        log.info("#######Starting Metric Report, Time = {}#######",
                System.currentTimeMillis());
        for (Entry<Gauge<?>, String> entry: gauges.entrySet()) {
            String gaugeName = entry.getValue();
            String content = entry.getKey().getValue().toString();
            if (gaugeName.endsWith("kms")) {
                int initSize = 50;
                PriorityQueue<ElementWithCount> heap 
                    = new PriorityQueue<ElementWithCount>(initSize, 
                            new Comparator<ElementWithCount>() {
                                public int compare(ElementWithCount ec1, 
                                        ElementWithCount ec2) {
                                    return (int)(ec1.getCount() 
                                            - ec2.getCount());
                                }
                            });
                Pattern p = Pattern.compile("\\[(.*?)\\]\\[(.*?)\\]");
                Matcher m = p.matcher(content);
                while (m.find()) {
                    String rawe = m.group(1);
                    String rawc = m.group(2);
                    String[] rawFeatures = rawe.split(",");
                    double[] features = new double[rawFeatures.length];
                    for (int i = 0; i < features.length; i++) {
                        features[i] 
                            = Double.parseDouble(rawFeatures[i].trim());
                    }
                    Element centroid = new Element(features);
                    long count = Long.parseLong(rawc.split(",")[1]);
                    heap.offer(new ElementWithCount(centroid, count));
                    if (heap.size() > initSize) {
                        heap.poll();
                    }
                }
                Element[] centroids = new Element[initSize];
                long[] counts = new long[initSize];
                for (int i = 0; i < centroids.length; i++) {
                    centroids[i] = heap.poll().getElement();
                }
                log.info("{}: {}", gaugeName, centroids);
            } else {
                log.info("{}: {}", gaugeName, content);
            }
        }
        log.info("#######Finished Metric Report#######");
    }

    private static class ElementWithCount {
        private final Element element;
        private final long count;
        public ElementWithCount(Element element, long count) {
            this.element = element;
            this.count = count;
        }
        public Element getElement() {
            return this.element;
        }
        public long getCount() {
            return this.count;
        }
    }
}
