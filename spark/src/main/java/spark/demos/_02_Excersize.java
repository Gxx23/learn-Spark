package spark.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.*;
import java.util.Comparator;

public class _02_Excersize {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer2");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("./spark_data/excersize_2/input/some.data");
        JavaRDD<String> rdd2 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                List<String> result = new ArrayList<>();
                try {
                    // 打印原始数据用于调试
                    System.out.println("Processing line: " + s);

                    // 先使用 JSONObject 进行基本解析验证
                    JSONObject jsonObj = JSON.parseObject(s);

                    // 检查关键字段是否存在
                    if (!jsonObj.containsKey("name") || !jsonObj.containsKey("sources")) {
                        System.err.println("Invalid JSON: missing 'name' or 'sources' field. Raw data: " + s);
                        return result.iterator();
                    }

                    // 再转换为 Student 对象
                    Student student = JSON.parseObject(s, Student.class);
                    int id = student.getId();
                    String name = student.getName();
                    List<Course> sources = student.getSources();

                    // 检查解析后的数据是否有效
                    if (name == null || name.isEmpty()) {
                        System.err.println("Invalid student name after parsing. Raw data: " + s);
                        return result.iterator();
                    }

                    if (sources == null) {
                        System.err.println("Student " + name + " has no courses (sources is null). Raw data: " + s);
                        return result.iterator();
                    }

                    // 处理课程数据
                    sources.stream()
                            .filter(course -> course != null
                                    && course.getName() != null
                                    && !course.getName().isEmpty())
                            .sorted(Comparator.comparingDouble(Course::getScore).reversed())
                            .limit(2)
                            .forEach(course -> {
                                String line = String.format("%d,%s,%s,%.2f",
                                        id, name, course.getName(), course.getScore());
                                result.add(line);
                            });
                } catch (Exception e) {
                    System.err.println("Fatal error processing line: " + s + ", error: " + e.getMessage());
                    e.printStackTrace(); // 打印完整堆栈信息
                }
                return result.iterator();
            }
        });

        rdd2.saveAsTextFile("./spark_data/excersize_2/output");
        sc.stop();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Student {
        private int id;
        private String name;
        private List<Course> sources;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Course {
        private int id;
        private String name;
        private double score;
    }
}
