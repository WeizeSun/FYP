package fyp;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.LinkedList;
import java.util.Collection;
import java.util.Arrays;

import java.io.File;

public class Query {

    private static int k;
    private static int maxIter = 100;

    private static Collection<Element> 
        kmeans(Collection<Element> elements) {

        Element[] centroids = new Element[k];
        Element[] elems 
            = elements.toArray(new Element[elements.size()]);
        for (int i = 0; i < centroids.length; i++) {
            centroids[i] = elems[i * centroids.length / k];
        }
        int[] cluster = new int[elements.size()];
        for (int iter = 0; iter < maxIter; iter++) {
            for (int i = 0; i < elems.length; i++) {
                double gm = Double.POSITIVE_INFINITY;
                int clu = 0;
                for (int j = 0; j < centroids.length; j++) {
                    double temp = elems[i].distance(centroids[i]);
                    if (temp < gm) {
                        clu = j;
                        gm = temp;
                    }
                }
                cluster[i] = clu;
            }

            for (int i = 0; i < centroids.length; i++) {
                double[] init = new double[k];
                Arrays.fill(init, 0.0);
                Element sum = new Element(init);
                int cnt = 0;
                for (int j = 0; j < elems.length; j++) {
                    if (cluster[j] == i) {
                        sum = sum.add(elems[j]);
                        cnt++;
                    }
                }
                centroids[i] = sum.scale(1.0 / cnt);
            }
        }
        return Arrays.asList(centroids);
    }

    private static class KMListener extends TailerListenerAdapter {
        private Pattern pattern = Pattern.compile("\\[(\\[.*?\\])\\]");
        @Override
        public void handle(String line) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                LinkedList<Element> elements = new LinkedList<Element>();
                String raw = matcher.group(1);
                Matcher m = Pattern.compile("\\[(.*?)\\]").matcher(raw);
                while (m.find()) {
                    String[] rawFeatures = m.group(1).split(",");
                    double[] features = new double[rawFeatures.length];
                    for (int i = 0; i < features.length; i++) {
                        features[i] = 
                            Double.parseDouble(rawFeatures[i].trim());
                    }
                    System.out.println(new Element(features));
                    elements.add(new Element(features));
                }
                Collection<Element> centroids = kmeans(elements);
                for (Element elem: centroids) {
                    System.out.println(elem);
                }
            }
        }
    }

    public static void main(String[] args) {
        k = Integer.parseInt(args[0]);
        if (args.length > 1) {
            maxIter = Integer.parseInt(args[1]);
        }
        TailerListener listener = new KMListener();
        Tailer tailer = new Tailer(
                new File("/home/weizesun/fuck.log"), listener, 500);
        tailer.run();
    }
}
