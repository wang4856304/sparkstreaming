package com.wj;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
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
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

//@SpringBootApplication
public class SparkStreamApp implements ApplicationContextAware {

    private static Logger log = LoggerFactory.getLogger(SparkStreamApp.class);
    private static ApplicationContext applicationContext;

    public static void main(String args[]) throws Exception {
        //new SpringApplicationBuilder().sources(SparkStreamApp.class).web(false).run(args);
        //SparkService sparkService = new SparkServiceImpl();
        //sparkService.runSpark();
        //SpringApplication.run(SparkStreamApp.class, args);
        /*BaseDao baseDao = applicationContext.getBean("baseDao", BaseDao.class);
        List<Map<String, Object>> map = baseDao.select(new HashMap<>());
        System.out.println(map);*/

        //kafkaProducer();
        //runSparkKafka();
        runSparkTest();
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

        words.foreachRDD(rdd-> {
            rdd.foreachPartition(iter-> {
                if (iter.hasNext()) {
                    iter.next();
                }
            });
            rdd.saveAsTextFile("/spark/data/test.log");
            //stringJavaRDD.saveAsTextFile("hdfs://10.50.20.171:9000/test.log");
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

    public static void runSparkTest() throws Exception {
        System.out.println(new JSONObject());
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
        //SparkConf sparkConf = new SparkConf().setMaster("spark://10.50.20.171:7077").setAppName("spark-streaming-test");//spark服务器配置
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("spark-streaming-test");
        sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");//当把它停止掉的时候，它会执行完当前正在执行的任务
        JavaStreamingContext jsc  = new JavaStreamingContext(sparkConf, Durations.seconds(20));//创建context上下文
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);//网络读取数据
        //JavaDStream<String> lines = jsc.textFileStream("D:\\test.txt");


        System.out.println("*********************************");


        /**flatMap**/
        //JavaDStream<String> javaDStreamFlatMap = lines.flatMap(str->Arrays.asList(str.split(",")).iterator());
        //javaDStreamFlatMap.print();

        /**mapToPair**/
        //JavaPairDStream<String, Integer>  javaDStreamMapToPair = javaDStreamFlatMap.mapToPair(str-> new Tuple2<>(str, 1));
        //javaDStreamMapToPair.print();

        /**reduceByKey**/
        //JavaPairDStream<String, Integer> reduceByKey= javaDStreamMapToPair.reduceByKey((x, y)-> x + y);
        //reduceByKey.print();




        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(fields);

        JavaDStream<Row> javaDStreamFlatMap = lines.flatMap(str->{
            List<Row> rowList = new ArrayList<>();
            if (str == null || str.length() == 0) {
                return rowList.iterator();
            }
            System.out.println(str);
            JSONArray jsonArray = JSONArray.parseArray(str);
            if (jsonArray != null && jsonArray.size() > 0) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    String sex = jsonObject.getString("sex");
                    int age = jsonObject.getInteger("age");
                    Row row = RowFactory.create(name, sex, age);
                    rowList.add(row);
                }
            }
            return rowList.iterator();
        });

        //javaDStreamFlatMap.filter(str->!str.equals("1"));
        javaDStreamFlatMap.foreachRDD(rdd->{
            rdd.foreachPartition(eachPartition->{
                //SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();// executed at the driver
                //List<Row> rowList = new ArrayList<>();


                //database info
                String url = "jdbc:mysql://localhost:3306/gateway?characterEncoding=utf8&useSSL=false";// executed at the worker
                Properties connectionProperties = new Properties();
                connectionProperties.put("user","root");
                connectionProperties.put("password","root");
                connectionProperties.put("driver","com.mysql.jdbc.Driver");


                //peopleDataFrame.write().mode("append").jdbc(url, "person", connectionProperties);
                eachPartition.forEachRemaining(row -> {
                    //rowList.add(row);

                    String sql = "insert into person(name, sex, age) values(" + "'" + row.getString(0) + "',"
                            + "'" + row.getString(1) + "'," + row.getInt(2) + ")";
                    Statement statement;
                    Connection conn = null;
                    try {
                        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/gateway?characterEncoding=utf8&useSSL=false",
                                "root",
                                "root");
                        statement = conn.createStatement();
                        statement.executeUpdate(sql);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("error");
                    }
                    finally {
                        try {
                           if (conn != null) {
                               conn.close();
                           }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
                //Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowList, schema);
                //peopleDataFrame.write().mode("append").jdbc(url, "person", connectionProperties);
            });
        });

        //JavaPairDStream<String, Integer> pair = javaDStreamMap.mapToPair(javaDStreamMap.map() -> new Tuple2<>(str, 1));

        lines.print();
        javaDStreamFlatMap.print();
        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
