package spark.demos;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import py4j.StringUtil;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class _02_Exercise_2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer4");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> datas = Arrays.asList(
                "a,1",
                "b,2",
                "a,2",
                "b,3",
                "a,3",
                "b,4"
        );

        //把一个内存集合转换成RDD
        JavaRDD<String> rdd1 = sc.parallelize(datas);

        // a,123
        // b,234
        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair(s -> {
            String[] split = s.split(",");
            return new Tuple2<>(split[0], Integer.parseInt(split[1]));
        });

        /*
        int sum = Integer.sum(1, 2);
        rdd2.reduceByKey(Integer::sum);
        rdd2.combineByKey(i -> 1, Integer::sum, Integer::sum)
         */

        //结果：a,list[1,2,3]
        JavaPairRDD<String, ArrayList<Integer>> rdd3 = rdd2.aggregateByKey(
                new ArrayList<Integer>(),
                (ArrayList<Integer> list, Integer value) -> {
                    list.add(value);
                    return list;
                },
                (ArrayList<Integer> list1, ArrayList<Integer> list2) -> {
                    list1.addAll(list2);
                    return list1;
                }
        );
        //输出：a,[123]
        rdd3.collect().forEach(tp -> System.out.println(tp._1() + ", [" + StringUtils.join(tp._2(), "") + "]"));
        sc.stop();


    }

}
