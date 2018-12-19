package com.wj.controller;

import com.wj.dao.BaseDao;
import com.wj.kafka.producer.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@RestController
public class TestController {

    //@Autowired
    //private BaseDao baseDao;

    @Autowired
    private DataSource hBasePhoenixDatasource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaProducerService kafkaProducerService;

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

    @RequestMapping("/producer")
    public Object kafkaProducer() {
        kafkaProducerService.sendMessage();
        return "success";
    }
}
