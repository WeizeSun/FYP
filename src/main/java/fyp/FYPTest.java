package fyp;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.common.functions.*;

public class FYPTest {
  public static void main(String[] args) throws Exception {
    String inputPath = args[0], outputPath = args[1];
    int parallelism = 1, kTarget = 50;
    final StreamExecutionEnvironment env
      = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Element> source = env.readTextFile(inputPath)
      .map(new MapFunction<String, Element>() {
        @Override
        public Element map(String line) throws Exception {
          String[] stringArray = line.split(",");
          double[] doubleArray = new double[stringArray.length];
          for (int i = 0; i < stringArray.length; i++) {
            doubleArray[i] = Double.parseDouble(stringArray[i]);
          }
          return new Element(doubleArray);
        }
      });

    DataStream<Tuple2<Long, Element>> result
      = new StreamingKCluster(50).getStream(source);

    result.writeAsText("/home/weizesun/sc/output.txt");
    env.execute("FYPTest");
  }
}
