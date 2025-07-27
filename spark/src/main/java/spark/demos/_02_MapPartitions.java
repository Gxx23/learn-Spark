package spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class _02_MapPartitions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("xer5");
        conf.setMaster("local");
        conf.set("spark.default.parallelism", "2");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.textFile("./spark_data/excersize_2/input/some.data");

        //每个分区创建一次对象
        JavaRDD<Order> resRdd = rdd1.mapPartitions(new QueryFlatMapFunction());
        resRdd.collect().forEach(System.out::println);
    }

    public static class QueryFlatMapFunction implements FlatMapFunction<Iterator<String>, Order> {
        @Override
        public Iterator<Order> call(Iterator<String> partitionIter) throws Exception {
            //创建后续处理数据时需要使用的工具
            //如创建jdbc连接
            Connection connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/test", "root", "123456");
            //设置查询模版
            PreparedStatement preparedStatement = connection.prepareStatement("select pname from product where pid = ?");

/*            ArrayList<String> result = new ArrayList<>();
            while (iterator.hasNext()) {
                String line = iterator.next();
                Order order = JSON.parseObject(line, Order.class);
                //获取pid,使用模版查询数据库
                preparedStatement.setInt(1, order.getPid()); // pid 代替第一个 ？
                ResultSet resultSet = preparedStatement.executeQuery();
                resultSet.next();
                String pname = resultSet.getString("pname");
                order.setPName(pname);
                result.add(JSON.toJSONString(order));
            }
            return result.iterator();*/
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
                        preparedStatement.setInt(1, order.getPid());
                        ResultSet resultSet = preparedStatement.executeQuery();
                        resultSet.next();
                        String pname = resultSet.getString("pname");
                        order.setPName(pname);

                        //判断分区后续是否还有数据要处理,如果没有则关闭数据库连接
                        if(!partitionIter.hasNext()){
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
        private int uid;
        private String oid;
        private int pid;
        private Double amount;
        private String pName;
    }

}
