package spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class _02_GroupByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer5");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<String, Integer>> datas = Arrays.asList(
                Tuple2.apply("spark", 1), Tuple2.apply("hive", 7), Tuple2.apply("hive", 4), Tuple2.apply("spark", 8),
                Tuple2.apply("spark", 3), Tuple2.apply("hbase", 2), Tuple2.apply("hbase", 6), Tuple2.apply("spark", 5));
        JavaPairRDD<String, Integer> pairRdd = sc.parallelizePairs(datas);
        //缓存pairRdd
        pairRdd.cache();
        //groupByKey() => 只分组，不聚合
        //pairRdd.groupByKey().collect().forEach(System.out::println);
        pairRdd.reduceByKey(Math::max).collect().forEach(System.out::println);
        pairRdd.reduceByKey(Integer::sum).collect().forEach(System.out::println);
        //求最大值，最小值，总和
        pairRdd.mapValues(v -> Tuple3.apply(v, v, v))
                .foldByKey(new Tuple3<>(Integer.MIN_VALUE, Integer.MAX_VALUE, 0), (acc, value) ->
                    new Tuple3<>(Math.max(acc._1(), value._1()), Math.min(acc._2(), value._2()), acc._3() + value._3())
                ).collect().forEach(System.out::println);
        //释放缓存
        pairRdd.unpersist();
        sc.stop();
    }
}
