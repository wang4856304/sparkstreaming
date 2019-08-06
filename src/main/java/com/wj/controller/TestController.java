package com.wj.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wj.dao.BaseDao;
import com.wj.kafka.producer.KafkaProducerService;
import com.wj.rabbitmq.SenderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@RestController
public class TestController {

    //@Autowired
    //private BaseDao baseDao;

    //@Autowired
    private DataSource hBasePhoenixDatasource;

    //@Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private SenderService senderService;

    @RequestMapping("/test")
    public Object test() throws Exception {
        //List<Map<String, Object>> map = baseDao.select(new HashMap<>());
        //System.out.println(map);
        Map<String, Object> map = new HashMap<>();
        map.put("idCardNum", 100);
        map.put("name", "wj");
        map.put("age", 34);
        map.put("identyNum", "adasdasd");
        //baseDao.insert(map);
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select IDCARDNUM idCardNum, \"column2\".\"Age\" age, \"column1\".\"Name\" name, \"column1\".\"identy_num\" identyNum from student");
        return "success";
    }

    @RequestMapping("/kafka/producer")
    public Object kafkaProducer(@RequestBody JSONArray jsonArray) {
        kafkaProducerService.sendMessage("test", jsonArray.toJSONString());
        return "success";
    }

    @RequestMapping("/sendMsg")
    public Object sendMQMsg(@RequestBody JSONObject jsonObject) {
        senderService.send("event", jsonObject.toJSONString());
        return "success";
    }


    public static void main(String args[]) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        //simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Instant time =Instant.parse("2019-07-25T05:11:17Z");
        System.out.println(time.plusMillis(TimeUnit.HOURS.toMillis(8)));
        Date date = simpleDateFormat.parse("2019-07-25T05:11:17Z");

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);

        //simpleDateFormat.setTimeZone(TimeZone.getDefault());
        //String str = simpleDateFormat.format(date.getTime());
        System.out.println(calendar.getTime());
    }

}
