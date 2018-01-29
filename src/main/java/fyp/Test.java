package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.streaming.api.*;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.*;

public class Test {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env 
            = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source 
            = env.readTextFile("/home/weizesun/a2.txt").uid("kmean");
        DataStream<Tuple2<Integer, Element>> parsed 
            = source.map(new Preprocess()).keyBy(0);
        DataStream<Integer> result 
            = parsed.map(new StreamingKmeans(50)).setParallelism(2);
        result.writeAsText("/home/weizesun/result.txt");
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
