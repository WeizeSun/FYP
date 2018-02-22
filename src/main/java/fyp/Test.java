package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;

public class Test {
    public static void main(String[] args) throws Exception {
        int k = 30;
        String inputPath;
        String outputPath;
        int parallelism = 1;
        int snapshotPeriod = 1000;
        String snapshotDirectory = "";
        if (args.length < 3) {
            throw new Exception("Parameter Error!");
        }
        k = Integer.parseInt(args[0]);
        inputPath = args[1];
        outputPath = args[2];
        if (args.length >= 4) {
            parallelism = Integer.parseInt(args[3]);
        }
        if (args.length >= 5) {
            snapshotPeriod = Integer.parseInt(args[4]);
        }
        if (args.length >= 6) {
            snapshotDirectory = args[5];
        }
        final StreamExecutionEnvironment env 
            = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source 
            = env.readTextFile(inputPath).uid("kmean");
        DataStream<Tuple2<Integer, Element>> parsed 
            = source.map(new Preprocess()).keyBy(0);
        DataStream<Integer> result
            = parsed
                .map(new StreamingKmeans(k, 
                            snapshotPeriod, snapshotDirectory))
                .setParallelism(parallelism);
        result.writeAsText(outputPath);
        env.execute("Test");
    }
}

class Preprocess implements MapFunction<String, Tuple2<Integer, Element>> {
    public Tuple2<Integer, Element> map(String value) {
        String[] stringArray = value.split(",");
        double[] doubleArray = new double[stringArray.length];
        for (int i = 0; i < stringArray.length; i++) {
            doubleArray[i] = Double.parseDouble(stringArray[i]);
        }
        return new Tuple2(0, new Element(doubleArray));
    }
}
