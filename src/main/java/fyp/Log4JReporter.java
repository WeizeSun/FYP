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

public class Log4JReporter implements MetricReporter, Scheduled {
    private final static Logger log 
        = LoggerFactory.getLogger(Log4JReporter.class);
    private final HashMap<Gauge<Iterable<Element>>, String> gauges 
        = new HashMap<Gauge<Iterable<Element>>, String>();

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
            if (metric instanceof Gauge) {
                gauges.put((Gauge)metric, name);
            }
        }
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric,
            String metricName, MetricGroup group) {
        synchronized(this) {
            if (metric instanceof Gauge) {
                gauges.remove(metric);
            }
        }
    }

    @Override
    public void report() {
        log.info("Starting Metric Report, Time = {}",
                System.currentTimeMillis());
        for (Entry<Gauge<Iterable<Element>>, String> entry: 
                gauges.entrySet()) {
            for (Element elem: entry.getKey().getValue()) {
                log.info("{}", elem);
            }
        }
    }
}
