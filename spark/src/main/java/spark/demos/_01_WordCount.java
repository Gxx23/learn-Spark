package spark.demos;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class _01_WordCount {
    public static void main(String[] args) {


        //构造Spark的参数对象，设置参数
        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("wordcount");

        //获取Spark的编程入口
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //把文件映射成RDD
        JavaRDD<String> rdd1 = jsc.textFile("./spark_data/wordcount/input/a.txt");
        //flatMap() -- 对数据映射成数组或集合，再把结果 压平 得到rdd中的元素
        JavaRDD<String> rdd2 = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] wordArray = s.split(" ");
                List<String> list = Arrays.asList(wordArray);
                return list.iterator();
            }
        });
//        JavaRDD<Tuple2<String, Integer>> rdd3 = rdd2.map(new Function<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<>(word, 1);
//            }
//        });

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });
        //shuffle
        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<String, Integer> resultRdd = rdd4.sortByKey();
        JavaRDD<String> formatres = resultRdd.map(tuple -> tuple._1() + ": " + tuple._2());
        //行动算子，触发计算任务真正执行
        //formatres.collect().forEach(System.out::println);
        formatres.saveAsTextFile("./spark_data/wordcount/output");

    }
}
