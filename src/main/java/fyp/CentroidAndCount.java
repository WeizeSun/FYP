package fyp;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Iterator;

public class CentroidAndCount {
    private Iterable<Element> centroids;
    private Iterable<Entry<Long, AtomicLong>> counts;

    public CentroidAndCount(Iterable<Element> centroids, 
            Iterable<Entry<Long, AtomicLong>> counts) {
        this.centroids = centroids;
        this.counts = counts;
    }

    public Iterable<Element> getCentroids() {
        return this.centroids;
    }

    public Iterable<Entry<Long, AtomicLong>> getCounts() {
        return this.counts;
    }

    @Override
    public String toString() {
        String ans = "";
        for (Entry<Long, AtomicLong> elem: counts) {
            ans += elem.getKey() + "," + elem.getValue().get() + ";";
        }
        return ans;
    }
}
