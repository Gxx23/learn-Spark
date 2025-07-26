package spark.demos;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.*;
import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class _02_AggregateBykey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer3");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_2/input/some.data");

        //把数据加工成K V对象
        //key : 课程名
        //value : 课程信息对象<CourseInfo>
        JavaPairRDD<String, CourseInfo> rdd2 = rdd1.flatMapToPair(new PairFlatMapFunction<String, String, CourseInfo>() {
            @Override
            public Iterator<Tuple2<String, CourseInfo>> call(String s) throws Exception {
                ArrayList<Tuple2<String, CourseInfo>> result = new ArrayList<>();
                JSONObject parseObject = JSON.parseObject(s);
                int id = parseObject.getIntValue("id");
                String name = parseObject.getString("name");
                JSONArray array = parseObject.getJSONArray("sources");

                for (int i = 0; i < array.size(); i++) {
                    JSONObject obj = array.getJSONObject(i);
                    String courseName = obj.getString("name");
                    double score = obj.getDoubleValue("score");
                    result.add(new Tuple2<>(courseName, new CourseInfo(id, name, courseName, score)));
                }
                return result.iterator();
            }
        });
        //按key分组聚合
        JavaPairRDD<String, Agg> rdd3 = rdd2.aggregateByKey(
                //创建初始累加器，对象类型Agg
                new Agg(),
                //进来一条数据，聚合数据，局部聚合
                new Function2<Agg, CourseInfo, Agg>() {
                    @Override
                    public Agg call(Agg agg, CourseInfo courseInfo) throws Exception {
                        //局部累加课程人数
                        agg.count += 1;
                        //局部累加课程总分
                        agg.sum += courseInfo.getScore();
                        return agg;
                    }
                },
                //多个局部聚合结果的合并，聚合累加器，全局聚合
                new Function2<Agg, Agg, Agg>() {
                    @Override
                    public Agg call(Agg agg, Agg agg2) throws Exception {
                        agg.count += agg2.count;
                        agg.sum += agg2.sum;
                        return agg;
                    }
                }
        );
        //输出聚合后的结果
        JavaRDD<String> rdd4 = rdd3.map(tuple2 -> tuple2._1 + " => " + tuple2._2.count + ", " + tuple2._2.sum);
        rdd4.saveAsTextFile("./spark_data/Aggregate_output");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CourseInfo implements Serializable {
        private int id;
        private String name;
        private String courseName;
        private double score;
    }

    /*
        自定义累加器：Agg
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {
        private int count;//课程人数
        private double sum;//课程总分
    }
}
