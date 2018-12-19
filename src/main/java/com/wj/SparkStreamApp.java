package com.wj;

import com.wj.dao.BaseDao;
import com.wj.kafka.producer.KafkaProducerService;
import com.wj.kafka.producer.impl.KafkaProducerServiceImpl;
import com.wj.spark.SparkService;
import com.wj.spark.impl.SparkServiceImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.hibernate.validator.constraints.SafeHtml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.*;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@SpringBootApplication
public class SparkStreamApp implements ApplicationContextAware {

    private static Logger log = LoggerFactory.getLogger(SparkStreamApp.class);
    private static ApplicationContext applicationContext;

    public static void main(String args[]) throws Exception {
        //new SpringApplicationBuilder().sources(SparkStreamApp.class).web(false).run(args);
        //SparkService sparkService = new SparkServiceImpl();
        //sparkService.runSpark();
        SpringApplication.run(SparkStreamApp.class, args);
        /*BaseDao baseDao = applicationContext.getBean("baseDao", BaseDao.class);
        List<Map<String, Object>> map = baseDao.select(new HashMap<>());
        System.out.println(map);*/

        //kafkaProducer();
        //runSparkKafka();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SparkStreamApp.applicationContext = applicationContext;
    }

    private static void kafkaProducer() {
        KafkaProducerService kafkaProducerService =  applicationContext.getBean("kafkaProducerServiceImpl", KafkaProducerService.class);
        kafkaProducerService.sendMessage();
    }

    public static void runSparkKafka() throws Exception {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
        SparkConf sparkConf = new SparkConf().setMaster("spark://10.50.20.171:7077").setAppName("spark-streaming-test");//spark服务器配置
        //SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("spark-streaming-test");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");//当把它停止掉的时候，它会执行完当前正在执行的任务。
        //String[] jars = new String[]{"E:\\IntelliJ IDEA 2018.2.5\\myProject\\avro\\target\\avro-1.0-SNAPSHOT.jar"};
        //sparkConf.setJars(jars);
        //sparkConf.set("spark.executor.memory", "512m");

        JavaStreamingContext jssc  = new JavaStreamingContext(sparkConf, Durations.seconds(10));//创建context上下文
        //JavaReceiverInputDStream<String> lines = jsc.socketTextStream("192.168.209.129", 9999);//网络读取数据

        String topics = "test";
        String brokers = "10.50.20.172:9092";
        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, Object> kafkaParams = new HashMap<>();
        //kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        //kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //kafkaParams.put("enable.auto.commit", false);//是否自动提交
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
        words.foreachRDD(rdd->new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {

                    }
                });
                stringJavaRDD.saveAsTextFile("/spark/data/test.log");
                //stringJavaRDD.saveAsTextFile("hdfs://10.50.20.171:9000/test.log");
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
        jssc.stop();
    }
}
