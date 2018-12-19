package com.wj.spark.impl;

import com.wj.kafka.producer.impl.KafkaProducerServiceImpl;
import com.wj.spark.SparkService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@Service
public class SparkServiceImpl implements SparkService {

    private static Logger log = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);

    //@PostConstruct
    @Override
    public void runSpark() throws Exception {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
        SparkConf sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("spark-streaming-test");//spark服务器配置
        String[] jars = new String[]{"E:\\IntelliJ IDEA 2018.2.5\\myProject\\avro\\target\\avro-1.0-SNAPSHOT.jar"};
        //sparkConf.setJars(jars);
        sparkConf.set("spark.executor.memory", "512m");
        JavaStreamingContext jsc  = new JavaStreamingContext(sparkConf, Durations.seconds(10));//创建context上下文
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.209.129", 9999);//网络读取数据
        //lines.print();
        //JavaDStream<String> lines = jsc.textFileStream("E:\\test");//本地读取数据

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });
        wordCount.print();
        jsc.start();
        jsc.awaitTermination();
    }

    @Override
    public void runSparkKafka() throws Exception {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("spark-streaming-test");//spark服务器配置
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");//当把它停止掉的时候，它会执行完当前正在执行的任务。
        //String[] jars = new String[]{"E:\\IntelliJ IDEA 2018.2.5\\myProject\\avro\\target\\avro-1.0-SNAPSHOT.jar"};
        //sparkConf.setJars(jars);
        //sparkConf.set("spark.executor.memory", "512m");

        JavaStreamingContext jssc  = new JavaStreamingContext(sparkConf, Durations.seconds(10));//创建context上下文
        //JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.209.129", 9999);//网络读取数据

        String topics = "test";
        String brokers = "192.168.209.130:9092";
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        //kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        //kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //Topic分区
        //Map<TopicPartition, Long> offsets = new HashMap<>();
        //offsets.put(new TopicPartition(topics, 0), 2L);

        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
        );

        /*JavaDStream<String> words = lines.flatMap(new FlatMapFunction<ConsumerRecord<Object,Object>, String>() {
            public Iterator<String> call(ConsumerRecord<Object,Object> s) throws Exception {
                return Arrays.asList(s.value().toString().split(",")).iterator();
            }
        });*/
        JavaDStream<String> wordX = lines.map(new Function<ConsumerRecord<Object, Object>, String>() {
            @Override
            public String call(ConsumerRecord<Object, Object> objectObjectConsumerRecord) throws Exception {
                return objectObjectConsumerRecord.value().toString();
            }
        });
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<ConsumerRecord<Object, Object>, String>() {
            @Override
            public Iterator<String> call(ConsumerRecord<Object, Object> objectObjectConsumerRecord) throws Exception {
                log.info("value={}", objectObjectConsumerRecord.toString());
                //System.out.println(objectObjectConsumerRecord.toString());
                return Arrays.asList(objectObjectConsumerRecord.value().toString()).iterator();
            }
        });
        words.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {

                    }
                });
                stringJavaRDD.saveAsTextFile("/spark/data");
            }
        });
        /*JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });*/
        //wordCount.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public void test() {

    }


    public static void main(String args[]) throws Exception {
        SparkService sparkService = new SparkServiceImpl();
        //sparkService.runSpark();
        sparkService.runSparkKafka();
    }
}
