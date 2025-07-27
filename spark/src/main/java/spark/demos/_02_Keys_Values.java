package spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class _02_Keys_Values {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer5");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> datas = Arrays.asList(
                Tuple2.apply("spark", 1), Tuple2.apply("hive", 7), Tuple2.apply("hive", 4), Tuple2.apply("spark", 8),
                Tuple2.apply("spark", 3), Tuple2.apply("hbase", 2), Tuple2.apply("hbase", 6), Tuple2.apply("spark", 5));
        JavaPairRDD<String, Integer> pairRdd = sc.parallelizePairs(datas);

        // keys: 获取所有的key
        pairRdd.keys().collect().forEach(System.out::println);
        // values: 获取所有的value
        pairRdd.values().collect().forEach(System.out::println);
        // mapValues: 对value进行操作
        pairRdd.mapValues(x -> x * 2).collect().forEach(System.out::println);
    }
}
