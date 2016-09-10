package distributedlossycounting.spark;

import com.google.common.base.Optional;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

/**
 * Created by ap0n on 1/20/15.
 */
public class LossyCounting {

  public static final double ERROR = 0.01 * 0.01;
  public static final double SUPPORT = 0.1 * 0.01;
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setMaster("local[*]")
        .setAppName("Distributted Lossy Counting");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(60));
//    jssc.checkpoint("/home/ap0n/workspace/master/distributed-lossy-counting/checkpoint");

    JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 8888);

//    JavaRDD<String> file
//        = jssc.sparkContext()
//        .textFile("/home/ap0n/workspace/master/distributed-lossy-counting/the_dataset.txt");

    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String x) {
        return Arrays.asList(SPACE.split(x));
      }
    });

    

    final Accumulator<Integer> n = jssc.sparkContext().accumulator(10);

    words.foreach(new Function<JavaRDD<String>, Void>() {
      @Override
      public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
        n.add(1);
        return null;
      }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        });

//    Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
//        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
//          @Override
//          public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
//            Integer newSum = 0;
//            for (Integer i : values) {
//              newSum += i;
//            }
//            if (state.isPresent()) {
//              return Optional.of(state.get() + newSum);
//            }
//            return Optional.of(newSum);
//          }
//        };
//    JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(updateFunction);

    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
        new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) throws Exception {
            return i1 + i2;
          }
        });

    // Print the results
    Function<JavaPairRDD<String, Integer>, Void>
        printFunction =
        new Function<JavaPairRDD<String, Integer>, Void>() {
          @Override
          public Void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
            List<Tuple2<String, Integer>> l = stringIntegerJavaPairRDD.collect();
            System.err.println("Print");
//        for (Tuple2<String, Integer> t : l) {
//          System.out.println(t.toString());
//        }
            System.err.println("Accumulator = " + n.value());
            return null;
          }
        };

    wordCounts.foreachRDD(printFunction);
//    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
