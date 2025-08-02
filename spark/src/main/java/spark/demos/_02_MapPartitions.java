package spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class _02_MapPartitions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer5");
        conf.setMaster("local");
        conf.set("spark.default.parallelism", "2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/exercise_3/input/order.data");

        //每个分区创建一次对象
        JavaRDD<Order> rdd2 = rdd1.mapPartitions(new QueryFlatMapFunction());
        //缓存数据库查询结果rdd1，避免多次连接数据库
        rdd1.cache();

        //优化代码，避免groupByKey的全量Shuffle造成的数据倾斜，减少排序时的内存开销
        /*
int maxSize = 2; // 假设取 Top2

// 1. CreateCombiner：初始化最小堆（按 usersSize 升序，堆顶是最小元素）
Function<Tuple2<String, Tuple3<Integer, Integer, Double>>,
    PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>> createCombiner = value -> {
        PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>> heap = new PriorityQueue<>(
            maxSize,
            (a, b) -> Integer.compare(a._2()._1(), b._2()._1()) // 升序，堆顶是最小 usersSize
    );
    heap.add(value);
    return heap;
};

// 2. MergeValue：同分区内合并，维护 TopN
BiFunction<PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>,
Tuple2<String, Tuple3<Integer, Integer, Double>>,
PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>> mergeValue = (heap, value) -> {
    if (heap.size() < maxSize) {
        heap.add(value);
    } else {
        // 新元素比堆顶大时，替换堆顶（保留更大的元素）
        if (value._2()._1() > heap.peek()._2()._1()) {
            heap.poll();
            heap.add(value);
        }
    }
    return heap;
};

// 3. MergeCombiners：跨分区合并堆，最终保留 TopN
BiFunction<PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>,
PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>,
PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>> mergeCombiners = (heap1, heap2) -> {
    PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>> merged = new PriorityQueue<>(
        maxSize,
        (a, b) -> Integer.compare(a._2()._1(), b._2()._1())
    );
    merged.addAll(heap1);
    merged.addAll(heap2);
    // 移除超出 maxSize 的最小元素
    while (merged.size() > maxSize) {
        merged.poll();
    }
    return merged;
};

// 应用 combineByKey
JavaPairRDD<String, PriorityQueue<Tuple2<String, Tuple3<Integer, Integer, Double>>>> combinedRdd = pairedRdd
    .combineByKey(createCombiner, mergeValue, mergeCombiners);

// 4. 转换为有序列表（堆是升序，反转成降序）
JavaPairRDD<String, List<Tuple2<String, Tuple3<Integer, Integer, Double>>>> topRdd = combinedRdd
    .mapValues(heap -> {
        List<Tuple2<String, Tuple3<Integer, Integer, Double>>> list = new ArrayList<>(heap);
        Collections.reverse(list); // 升序堆反转成降序
        return list;
    });
         */

        // 查询各品牌中，购买的订单数最多的前两种商品，及其购买订单数、人数、总金额
        // 按品牌+商品名进行分组
        JavaPairRDD<Tuple2<String, String>, Tuple3<Integer, Integer, Double>> formattedRdd =
                rdd2.mapToPair(order -> new Tuple2<>(new Tuple2<>(order.getBrand(), order.getProduct_name()), order))
                        .aggregateByKey(
                                new Agg(),
                                (agg, order) -> {
                                    agg.count++;
                                    agg.users.add(order.getUser_id());
                                    agg.sum += order.getAmount();
                                    return agg;
                                },
                                (agg1, agg2) -> {
                                    agg1.count += agg2.count;
                                    agg1.users.addAll(agg2.users);
                                    agg1.sum += agg2.sum;
                                    return agg1;
                                }
                        ).mapValues(agg -> new Tuple3<>(agg.count, agg.users.size(), agg.sum));

        JavaRDD<Tuple2<String, List<Tuple2<String, Tuple3<Integer, Integer, Double>>>>> topRdd =
                formattedRdd.mapToPair(tuple -> {
                            String brand = tuple._1()._1();
                            String productName = tuple._1()._2();
                            return new Tuple2<>(brand, new Tuple2<>(productName, tuple._2()));
                        }).groupByKey()
                        .map(tuple -> {
                            // 将迭代器转换为列表
                            String brand = tuple._1();
                            ArrayList<Tuple2<String, Tuple3<Integer, Integer, Double>>> aggList = new ArrayList<>();
                            tuple._2.forEach(aggList::add);
                            aggList.sort((a, b) -> b._2()._1() - a._2()._1());
                            List<Tuple2<String, Tuple3<Integer, Integer, Double>>> top2 = aggList.stream().limit(2).collect(Collectors.toList());
                            return new Tuple2<>(brand, top2);
                        });
        topRdd.foreach(
                tuple -> {
                    System.out.println("品牌: " + tuple._1());
                    tuple._2.forEach(product -> {
                        System.out.println(
                                "商品名：" + product._1() +
                                        "订单数: " + product._2()._1() +
                                        ", 去重用户数: " + product._2()._2() +
                                        ", 总金额: " + product._2()._3()
                        );
                    });
                }
        );
    }

    // 查询数据库，补全数据(品牌，商品名)
    public static class QueryFlatMapFunction implements FlatMapFunction<Iterator<String>, Order> {
        @Override
        public Iterator<Order> call(Iterator<String> partitionIter) throws Exception {
            //Stream.generate(() -> partitionIter.hasNext() ? partitionIter.next() : null).takeWhile(line -> line != null)

            //创建后续处理数据时需要使用的工具
            //如创建jdbc连接
            Connection connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/mysql_test", "root", "123456");
            //设置查询模版
            PreparedStatement preparedStatement = connection.prepareStatement("select brand, product_name from product where product_id = ?");
            //改进代码 - refine, 使用输入的 partitionIterator迭代器 处理数据后返回，减少list的内存占用
            return new Iterator<Order>() {
                @Override
                public boolean hasNext() {
                    // 判断 partitionIter 是否有下一个元素
                    return partitionIter.hasNext();
                }

                @Override
                public Order next() {
                    // 对 partitionIter 数据处理后返回
                    String line = partitionIter.next();
                    Order order = JSON.parseObject(line, Order.class);
                    try {
                        //获取pid,使用模版查询数据库
                        preparedStatement.setInt(1, order.getProduct_id());
                        ResultSet resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        String brand = resultSet.getString("brand");
                        String pName = resultSet.getString("product_name");
                        order.setBrand(brand);
                        order.setProduct_name(pName);

                        //判断分区后续是否还有数据要处理,如果没有则关闭数据库连接
                        if (!partitionIter.hasNext()) {
                            preparedStatement.close();
                            connection.close();
                        }
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    return order;
                }
            };
        }
    }

    //订单对象
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private int order_id;
        private int product_id;
        private String user_id;
        private String brand;
        private String product_name;
        private Double amount;
        private Date order_date;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {
        private int count;
        private HashSet<String> users = new HashSet<>();
        private double sum;
    }

}
