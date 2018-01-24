package fyp;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.util.LinkedList;

import java.io.File;

public class Query {

    private static int k;

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
                    System.out.println(m.group(1));
                    String[] rawFeatures = m.group(1).split(",");
                    double[] features = new double[rawFeatures.length];
                    for (int i = 0; i < features.length; i++) {
                        features[i] = 
                            Double.parseDouble(rawFeatures[i].trim());
                    }
                    System.out.println(new Element(features));
                    elements.add(new Element(features));
                }
            }
        }
    }

    public static void main(String[] args) {
        k = Integer.parseInt(args[0]);
        TailerListener listener = new KMListener();
        Tailer tailer = new Tailer(
                new File("/home/weizesun/fuck.log"), listener, 500);
        tailer.run();
    }
}
