package com.wj.spark.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wj.entity.Warnning;
import com.wj.queueenum.QueueEnum;
import com.wj.rabbitmq.SenderService;
import com.wj.repository.WarningRepository;
import com.wj.utils.DateUtil;
import com.wj.utils.FileUtil;
import com.wj.utils.SpringContextUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;

/**
 * @author jun.wang
 * @title: CountServiceImpl
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/7/26 14:09
 */

@Service("countService")
public class CountServiceImpl extends AbstractSparkService {

    private static Logger log = LoggerFactory.getLogger(CountServiceImpl.class);
    private static final String checkPointDir = "/spark/warning_count";

    private static final String textFilePath = "d:\\event\\data.txt";

    @Override
    public void runSpark() throws Exception {

        System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-2.7.1");
        String hostName = "local[2]";
        String appName = "spark-streaming-count";
        Map<String, String> prop = new HashMap<>();
        prop.put("spark.executor.memory", "512m");
        JavaStreamingContext jsc = createSparkContext(hostName, appName, checkPointDir, Durations.seconds(20), prop);
        jsc.checkpoint(checkPointDir);//spark 持久化容错目录设置

        String topics = "test";
        String brokers = "127.0.0.1:9092";
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        //kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        //kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("enable.auto.commit", false);//是否自动提交

        JavaInputDStream<ConsumerRecord<Object, Object>> kafkaRecord = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );

        JavaDStream<String> lines = kafkaRecord.map(record -> record.value().toString());


        JavaDStream<Warnning> warnningJavaDStream = lines.flatMap(str -> {
            List<Warnning> warnningList = new ArrayList<>();
            if (str == null || str.length() == 0) {
                return warnningList.iterator();
            }
            JSONArray jsonArray = JSONArray.parseArray(str);
            if (jsonArray != null && jsonArray.size() > 0) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    String tid = jsonObject.getString("tid");
                    String mid = jsonObject.getString("mid");
                    String did = jsonObject.getString("did");
                    Warnning warnning = new Warnning();
                    warnning.setTId(tid);
                    warnning.setMId(mid);
                    warnning.setDId(did);
                    warnningList.add(warnning);
                }
            }
            return warnningList.iterator();
        });


        JavaPairDStream<Warnning, Integer> warnningPairStream = warnningJavaDStream.mapToPair(warnning -> new Tuple2<>(warnning, 1));
        JavaPairDStream<Warnning, Integer> reducePairStream = warnningPairStream.reduceByKey((a, b)->a + b);
        Function3<Warnning, Optional<Integer>, State<Integer>, Tuple2<Warnning, Integer>> mappingFunction = new Function3<Warnning, Optional<Integer>, State<Integer>, Tuple2<Warnning, Integer>>() {

            /**
             * serialVersionUID
             */
            private static final long serialVersionUID = -4105602513005256270L;

            // curState为当前key对应的state
            @Override
            public Tuple2<Warnning, Integer> call(Warnning key, Optional<Integer> value,
                               State<Integer> curState) throws Exception {
                Integer sum = value.orElse(0) +(curState.exists()? curState.get(): 0);
                Tuple2<Warnning, Integer> output = new Tuple2<>(key, sum);
                curState.update(sum);
                return output;

            }
        };
        JavaMapWithStateDStream<Warnning, Integer, Integer, Tuple2<Warnning, Integer>> warningCounts = reducePairStream.mapWithState(StateSpec.function(mappingFunction));
        warningCounts.cache();//缓存到内存
        warningCounts.checkpoint(Durations.seconds(100));//持久化到文件系统

        JavaDStream<Warnning> warnningCount = warningCounts.map((tuple2) -> {
            Warnning warnning = tuple2._1;
            int times = tuple2._2;
            warnning.setTimes(times);
            warnning.setCreateAt(DateUtil.formartDate(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));
            warnning.setUpdateAt(DateUtil.formartDate(new Date(), DateUtil.YYYY_MM_DD_HH_MM_SS));
            return warnning;
        });
        /*JavaPairDStream<Warnning, Integer> warnningUpdateStateByKeyPairStream = warnningPairStream.updateStateByKey((values, state) -> {
            Integer newValue = 0;
            //判断state是否存在，如果不存在，说明是一个key第一次出现
            //如果存在，说明这个key之前已经统计过全局的次数了
            if (state.isPresent()) {
                newValue = state.get();
            }

            //将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
            for (Integer v : values) {
                newValue += v;
            }
            return Optional.of(newValue);
        });

        warnningUpdateStateByKeyPairStream.cache();//缓存到内存
        warnningUpdateStateByKeyPairStream.checkpoint(Durations.seconds(100));//持久化到文件系统

        JavaDStream<Row> warnningCount = warnningUpdateStateByKeyPairStream.flatMap((tuple2) -> {
            List<Row> rowList = new ArrayList<>();
            Warnning warnning = tuple2._1;
            int times = tuple2._2;
            Row row = RowFactory.create(warnning.getTId(), warnning.getMId(), warnning.getDId(), times);
            rowList.add(row);
            return rowList.iterator();
        });*/

        warnningCount.foreachRDD(rdd -> {
            rdd.foreachPartition(eachPartition -> {
                eachPartition.forEachRemaining(warnning -> {
                    //数据入mq
                    SenderService senderService = SpringContextUtil.getBean("senderServiceImpl", SenderService.class);
                    senderService.send(QueueEnum.EVENT.getQueueName(), JSONObject.toJSONString(warnning));
                });
            });
        });

        //driver服务重启时，恢复原有流程或数据
        JavaStreamingContext backupJsc = JavaStreamingContext.getOrCreate(checkPointDir, () -> jsc);
        lines.print();
        backupJsc.start();
        backupJsc.awaitTermination();

    }

    public void addWarning(Row row, Connection connection) {
        String addSql = "insert into event(tid, mid, did, times) values(" + "'" + row.getString(0) + "',"
                + "'" + row.getString(1) + "'," + "'" + row.getString(2) + "'," + row.getInt(3) + ")";
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(addSql);
        } catch (Exception e) {
            log.error("addWarning error", e);
            throw new RuntimeException("addWarning error", e);
        }
    }

    @Override
    public void runSparkKafka() throws Exception {

    }

    @Override
    public String getDataBySql(String sql) {
        return null;
    }

    @RabbitListener(queues = "event")
    public void getEvent(String msg) {
        Warnning warnning = JSONObject.parseObject(msg, Warnning.class);
        WarningRepository warningRepository = SpringContextUtil.getBean("warningRepository", WarningRepository.class);
        warningRepository.save(warnning);
    }
}
