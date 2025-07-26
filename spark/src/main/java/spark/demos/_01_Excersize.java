package spark.demos;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.Arrays;
import java.util.Iterator;

/*
横表变成竖表
 */
public class _01_Excersize {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer1");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("./spark_data/excersize_1/input/source.data");
        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                //解析json, 获取对应数据
                int id = jsonObject.getIntValue("id");
                String name = jsonObject.getString("name");
                JSONArray sources = jsonObject.getJSONArray("sources");
                //结果数组，返回迭代器
                String[] result = new String[sources.size()];

                for (int i = 0; i < sources.size(); i++) {
                    result[i] = id + "," + name + "," + sources.getString(i);
                }

                return Arrays.stream(result).iterator();
            }
        });

        rdd2.saveAsTextFile("./spark_data/excersize_1/output");
    }
}
