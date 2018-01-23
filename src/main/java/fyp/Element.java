package fyp;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;

public class Element {
    private double features[];
    public final int size;
    public final int length;
    public Element(double[] array) {
        this.size = array.length;
        this.length = array.length;
        this.features = new double[this.size];
        for (int i = 0; i < this.size; i++) {
            this.features[i] = array[i];
        }
    }
    public Element(double head, double... feature) {
        this.size = feature.length;
        this.length = feature.length;
        this.features = new double[this.size];
        this.features[0] = head;
        for (int i = 1; i < this.size; i++) {
            this.features[i] = feature[i - 1];
        }
    }
    public Element(Collection<Double> feature) {
        this.size = feature.size();
        this.length = feature.size();
        this.features = new double[this.size];
        Iterator<Double> it = feature.iterator();
        int pos = 0;
        while (it.hasNext()) {
            this.features[pos++] = it.next();
        }
    }
    public static double minDistanceInSet(Collection<Element> elements) {
        int len = elements.size();
        double ans = -1;
        ArrayList<Element> elementsArray =
            new ArrayList<Element>(elements);
        for (int i = 0; i < len; i++) {
            for (int j = i + 1; j < len; j++) {
                double temp = 
                    elementsArray.get(i).distance(elementsArray.get(j));
                ans = ans < 0 || ans > temp?temp:ans;
            }
        }
        return ans;
    }
    public static Collection<Double> 
        allDistancesInSet(Collection<Element> elements) {
        int len = elements.size();
        LinkedList<Double> ans = new LinkedList<Double>();
        ArrayList<Element> elementsArray = 
            new ArrayList<Element>(elements);
        for (int i = 0; i < len; i++) {
            for (int j = i + 1; j < len; j++) {
                ans.add(elementsArray.get(i).
                        distance(elementsArray.get(j)));
            }
        }
        return (Collection<Double>) ans;
    }
    public Element subtract(Element that) {
        double temp[] = new double[Math.max(this.size, that.size)];
        if (this.size != that.size) {
            System.out.println("Warning: two elements have different sizes,"
                    + " will automatically paddle the shorter one with 0s");
        }
        for (int i = 0; i < Math.max(this.size, that.size); i++) {
            double x1 = this.size <= i?0:features[i];
            double x2 = that.size <= i?0:that.get(i);
            temp[i] = x1 - x2;
        }
        return new Element(temp);
    }
    public Element add(Element that) {
        double temp[] = new double[Math.max(this.size, that.size)];
        if (this.size != that.size) {
            System.out.println("Warning: two elements have different sizes,"
                    + " will automatically paddle the shorter one with 0s");
        }
        for (int i = 0; i < Math.max(this.size, that.size); i++) {
            double x1 = this.size <= i?0:features[i];
            double x2 = that.size <= i?0:that.get(i);
            temp[i] = x1 + x2;
        }
        return new Element(temp);
    }
    public double distance(Element that) {
        Element temp = this.subtract(that);
        double ans = 0;
        for (int i = 0; i < temp.size; i++) {
            ans += temp.get(i) * temp.get(i);
        }
        return Math.sqrt(ans);
    }
    public double get(int pos) {
        return features[pos];
    }
}
