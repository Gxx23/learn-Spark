package spark.demos;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class X1_FunnelModel {
    //- 漏斗模型
    //- 各设备类型下，漏斗模型中各步骤的：触达人数，相对转化率，绝对转化率
    //定义了一条业务路径（漏斗模型）为：
    private static final String[] FUNNEL_STEPS = {"e03","e05","e06"};
    private static final String inputPath = "./spark_data/funnel/input/funnel.data";
    private static final String outputPath = "./spark_data/funnel/output/";
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName("FunnelModel");
        conf.setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> data = sc.textFile(inputPath);
        FileUtils.deleteDirectory(new File(outputPath));
data.count();
        JavaPairRDD<Long, UserEvent> kvRdd = data.mapToPair(line -> {
            UserEvent userEvent = JSON.parseObject(line, UserEvent.class);
            return new Tuple2<>(userEvent.uid, userEvent);
        });
        //按uid进行分组，每组为一条结果
        JavaPairRDD<Long, Iterable<UserEvent>> groupRdd = kvRdd.groupByKey();
        // 3. 处理每个用户的事件序列，筛选漏斗路径 + 按设备类型聚合
        //    输出格式：(device_type, (步骤序列, 用户数))
        JavaPairRDD<String, List<String>> funnelPathRdd = groupRdd.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<UserEvent>>, String, List<String>>() {
            @Override
            public Iterator<Tuple2<String, List<String>>> call(Tuple2<Long, Iterable<UserEvent>> tuple2) throws Exception {
                Long uid = tuple2._1;
                Iterable<UserEvent> events = tuple2._2;
                // 转换为 List 并按时间排序
                ArrayList<UserEvent> SortedEvents = new ArrayList<>();
                events.forEach(SortedEvents::add);
                SortedEvents.sort(Comparator.comparingLong(UserEvent::getEvent_time));
                List<String> funnelPath = extractFunnelSteps(SortedEvents);
                // 如果有有效路径，按设备类型输出
                if (!funnelPath.isEmpty())
                    return Collections.singletonList(new Tuple2<>(SortedEvents.get(0).device_type, funnelPath)).iterator();
                else
                    return Collections.emptyIterator();
            }
        });

        // 4. 按设备类型分组，统计各步骤数据
        JavaPairRDD<String, Iterable<List<String>>> deviceGroupRdd = funnelPathRdd.groupByKey();
        JavaRDD<Object> resultRdd = deviceGroupRdd.map(deviceTuple -> {
            String deviceType = deviceTuple._1;
            Iterable<List<String>> allPaths = deviceTuple._2;
            HashMap<String, Integer> stepCount = new HashMap<>();
            for (String funnelStep : FUNNEL_STEPS) {
                stepCount.put(funnelStep, 0);
            }
            allPaths.forEach(paths -> {
                for (String path : paths) {
                    stepCount.put(path, stepCount.get(path) + 1);
                }
            });
            return calculateFunnelMetrics(deviceType, stepCount);
        });
        resultRdd.collect().forEach(System.out::println);
        resultRdd.saveAsTextFile("./spark_data/funnel/output/");
        sc.stop();
    }

    // 辅助方法：计算漏斗指标（触达人数、相对转化率、绝对转化率）
    private static String calculateFunnelMetrics(String deviceType, HashMap<String, Integer> stepCount) {
        StringBuilder result = new StringBuilder();
        result.append("设备类型：").append(deviceType).append("\n");

        Integer firstStepCount = stepCount.get(FUNNEL_STEPS[0]);
        Integer prevStepCount=  firstStepCount;

        for (int i = 0; i < FUNNEL_STEPS.length; i++) {
            String step = FUNNEL_STEPS[i];
            Integer currentCount = stepCount.get(step);

            // 触达人数
            result.append("步骤").append(i + 1).append("[").append(step).append("]: ").append(currentCount).append("\n");

            // 相对转化率
            if (i > 0){
                double relativeRate = prevStepCount == 0 ? 0 : (double) currentCount / prevStepCount;
                result.append("  相对转化率：").append(String.format("%.2f", relativeRate * 100)).append("\n");
            }

            // 绝对转化率
            double absoluteRate = firstStepCount == 0 ? 0 : (double) currentCount / firstStepCount;
            result.append("  绝对转化率：").append(String.format("%.2f", absoluteRate * 100)).append("\n");

            prevStepCount = currentCount;
        }
        return result.toString();
    }

    // 辅助方法：从事件序列中提取符合漏斗步骤的路径
    private static List<String> extractFunnelSteps(List<UserEvent> events) {
        List<String> result = new ArrayList<>();
        int stepIndex = 0;
        for (UserEvent userEvent : events) {
            if(FUNNEL_STEPS[stepIndex].equals(userEvent.event_id)){
                result.add(userEvent.event_id);
                stepIndex++;
                // 已匹配完所有步骤，提前终止
                if (stepIndex == FUNNEL_STEPS.length) break;
            }
        }
        return result;
    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserEvent implements Serializable {
        //{"uid":1,"event_time":1725886211000,"event_id":"e03","properties":{"url":"/aaa/bbb"},"device_type":"android"}
        private long uid;
        private long event_time;
        private String event_id;
        private String device_type;
    }
}
