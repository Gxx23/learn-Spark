package spark.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class _03_coGroup {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("xer3");
        conf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // ------造一个 rdd1------
        List<Student> lst1 = Arrays.asList(
                new Student(1, "aa", 18),
                new Student(2, "bb", 28),
                new Student(3, "cc", 38),
                new Student(4, "dd", 26)
        );
        JavaRDD<Student> rdd1 = sc.parallelize(lst1);

        // ------造一个 rdd2------
        List<Scores> lst2 = Arrays.asList(
                new Scores(1, "c1", 180),
                new Scores(1, "c2", 108),
                new Scores(1, "c3", 98),
                new Scores(2, "c1", 120),
                new Scores(2, "c2", 230),
                new Scores(2, "c4", 300),
                new Scores(3, "c1", 300),
                new Scores(3, "c3", 266),
                new Scores(3, "c4", 244)
        );
        JavaRDD<Scores> rdd2 = sc.parallelize(lst2);

        // 把两个 rdd 变成 KV RDD
        JavaPairRDD<Integer, Student> kvRdd1 = rdd1.keyBy(Student::getId);
        JavaPairRDD<Integer, Scores> kvRdd2 = rdd2.keyBy(Scores::getSid);

        // 协同分组：必须针对 KV RDD
        JavaPairRDD<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> coGrouped = kvRdd1.cogroup(kvRdd2);

        //实现学生表 和 分数表的关联
        //flatMap 是在每个分区内处理数据，排序只会对每个分区内的数据生效，而不会对整个 RDD 的结果进行全局排序。
        JavaRDD<Joined> resultRdd = coGrouped.flatMap(new FlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>>, Joined>() {
            @Override
            public Iterator<Joined> call(Tuple2<Integer, Tuple2<Iterable<Student>, Iterable<Scores>>> input) throws Exception {
                Iterable<Student> students = input._2._1;
                Iterable<Scores> scores = input._2._2;
                ArrayList<Joined> joineds = new ArrayList<>();
                for (Student student : students) {
                    for (Scores score : scores) {
                        joineds.add(new Joined(student.getId(), student.getName(), student.getAge(), score.getCourse(), score.getScore()));
                    }
                }
                return joineds.iterator();
            }
        });
        // 创建自定义可比较键类用于排序
        class SortKey implements Comparable<SortKey>, Serializable {
            private final int sid;
            private final double score;

            public SortKey(int sid, double score) {
                this.sid = sid;
                this.score = score;
            }

            @Override
            public int compareTo(SortKey other) {
                int sidComparison = Integer.compare(this.sid, other.sid);
                return (sidComparison == 0) ? Double.compare(other.score, this.score) : sidComparison;
            }
        }
        JavaRDD<Joined> sortedRdd = resultRdd.sortBy(joined -> new SortKey(joined.getSid(), joined.getScore()),
                true,
                1
        );
        sortedRdd.collect().forEach(System.out::println);
    }

    @Data
    @           NoArgsConstructor
    @AllArgsConstructor
    public static class Student implements Serializable {
        private int id;
        private String name;
        private int age;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Scores implements Serializable {
        private int sid;
        private String course;
        private double score;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Joined implements Serializable {
        private int sid;
        private String name;
        private int age;
        private String course;
        private double score;
    }
}
