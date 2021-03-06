package com.wj.spark.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wj.kafka.producer.impl.KafkaProducerServiceImpl;
import com.wj.queueenum.QueueEnum;
import com.wj.rabbitmq.SenderService;
import com.wj.utils.SpringContextUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@Service
public class SparkServiceImpl extends AbstractSparkService {

    private static Logger log = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);
    private static final String checkPointDir = "/spark/checkponit";


    @Override
    public void runSpark() throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin-master");
        //SparkConf sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("spark-streaming-test");//spark服务器配置
        //SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("spark-streaming-test");
        //sparkConf.set("spark.executor.memory", "512m");
        //JavaStreamingContext jsc  = new JavaStreamingContext(sparkConf, Durations.seconds(20));//创建context上下文
        //jsc.checkpoint(checkPointDir);//spark 持久化容错目录设置
        //driver服务重启时，恢复原有流程或数据
        //JavaStreamingContext backupJsc = JavaStreamingContext.getOrCreate(checkPointDir, ()->jsc);

        String hostName = "local[2]";
        String appName = "spark-streaming-test";
        Map<String, String> prop = new HashMap<>();
        prop.put("spark.executor.memory", "512m");
        JavaStreamingContext jsc = createSparkContext(hostName, appName, checkPointDir, Durations.seconds(20), prop);
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("127.0.0.1", 9999);//网络读取数据

        JavaDStream<Row> javaDStreamFlatMap = lines.flatMap(this::exchangePersonInfo);

        javaDStreamFlatMap.foreachRDD(rdd->{
            rdd.foreachPartition(eachPartition->{
                //ConnectionPool connectionPool = SpringContextUtil.getBean("connectionPool", ConnectionPool.class);
                DataSource dataSource = SpringContextUtil.getBean(DataSource.class);
                Connection connection = dataSource.getConnection();
                List<Row> rowList = new ArrayList<>();
                eachPartition.forEachRemaining(row -> {
                    rowList.add(row);
                    insertPerson(row, connection);
                });
                //createSparkSql(rowList);
            });
        });

        JavaDStream<String> namesStreamFlatMap = lines.flatMap(str->{
            List<String> names = new ArrayList<>();
            JSONArray jsonArray = JSONArray.parseArray(str);
            if (jsonArray != null && jsonArray.size() > 0) {
                for (int i = 0; i < jsonArray.size(); i++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(i);
                    String name = jsonObject.getString("name");
                    names.add(name);
                }
            }
            return names.iterator();
        });

        JavaPairDStream<String, Integer> namePairStream = namesStreamFlatMap.mapToPair(str->new Tuple2<>(str, 1));
        //每隔10秒计算前30秒的数据
        JavaPairDStream<String, Integer> nameReduceByKeyAndWindowPairStream = namePairStream.reduceByKeyAndWindow((x, y)->x+y, Durations.seconds(60), Durations.seconds(20));
        JavaPairDStream<String, Integer> nameUpdateStateByKeyPairStream = namePairStream.updateStateByKey((values, state)->{
            Integer newValue = 0;
            //判断state是否存在，如果不存在，说明是一个key第一次出现
            //如果存在，说明这个key之前已经统计过全局的次数了
            if (state.isPresent()) {
                newValue = state.get();
            }

            //将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
            for (Integer v: values) {
                newValue += v;
            }
            return Optional.of(newValue);
        });

        nameUpdateStateByKeyPairStream.cache();//缓存到内存
        nameUpdateStateByKeyPairStream.checkpoint(Durations.seconds(20));//持久化到文件系统

        JavaDStream<Row> nameCount = nameUpdateStateByKeyPairStream.flatMap((tuple2)->{
            List<Row> rowList = new ArrayList<>();
            //数据入mq
            SenderService senderService = SpringContextUtil.getBean("senderServiceImpl", SenderService.class);
            JSONObject json = new JSONObject();
            json.put("name", tuple2._1);
            json.put("count", tuple2._2);
            senderService.send(QueueEnum.NAME_COUNT.getQueueName(), json.toJSONString());

            Row row = RowFactory.create(tuple2._1, tuple2._2);
            rowList.add(row);
            return rowList.iterator();
        });

        nameCount.foreachRDD(rdd->{
            rdd.foreachPartition(eachPartition->{
                //ConnectionPool connectionPool = SpringContextUtil.getBean("connectionPool", ConnectionPool.class);
                DataSource dataSource = SpringContextUtil.getBean(DataSource.class);
                Connection connection = dataSource.getConnection();
                eachPartition.forEachRemaining(row -> updateNameCount(row, connection));
            });
        });

        lines.print();
        jsc.start();
        jsc.awaitTermination();
    }

    private Iterator<Row> exchangePersonInfo(String var1) {
        List<Row> rowList = new ArrayList<>();
        if (var1 == null || var1.length() == 0) {
            return rowList.iterator();
        }
        JSONArray jsonArray = JSONArray.parseArray(var1);
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
    }

    private void insertPerson(Row row, Connection connection) {
        String selectSql = "select name from person where name=" + "'" + row.getString(0) + "'";
        String addSql = "insert into person(name, sex, age) values(" + "'" + row.getString(0) + "',"
                + "'" + row.getString(1) + "'," + row.getInt(2) + ")";
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql);
            if (resultSet == null|| !resultSet.next()) {
                statement.executeUpdate(addSql);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateNameCount(Row row, Connection connection) {
        String selectSql = "select name from person_count where name=" + "'" + row.getString(0) + "'";
        String addSql = "insert into person_count(name, count) values(" + "'" + row.getString(0) + "',"
                + row.getInt(1) + ")";
        String updateSql = "update person_count set count=" + row.getInt(1) + " where name="+ "'" + row.getString(0) + "'";
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(selectSql);
            if (resultSet == null|| !resultSet.next()) {
                statement.executeUpdate(addSql);
            }
            else {
                statement.executeUpdate(updateSql);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createSparkSql(List<Row> rowList) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        List<StructField> structFields=new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("sex",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType=DataTypes.createStructType(structFields);
        Dataset<Row> dataset = sparkSession.createDataFrame(rowList, structType);
        try {
            dataset.createOrReplaceTempView("person");
            Dataset<Row> result = sparkSession.sql("select * from person");
            result.show();
        }
        catch (Exception e) {
            throw new RuntimeException("createOrReplaceTempView error", e);
        }
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

    @Override
    public String getDataBySql(String sql) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        Dataset<Row> result = sparkSession.sql(sql);
        return result.toString();
    }
}
