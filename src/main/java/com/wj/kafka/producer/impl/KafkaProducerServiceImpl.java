package com.wj.kafka.producer.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wj.kafka.producer.KafkaProducerService;
import com.wj.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@Service
public class KafkaProducerServiceImpl implements KafkaProducerService {

    private static Logger log = LoggerFactory.getLogger(KafkaProducerServiceImpl.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @Override
    public void sendMessage() {
        String message = "";
        try {
            message = FileUtil.readJson("E:\\IntelliJ IDEA 2018.2.5\\myProject\\xsy-bigdata\\test-data-related\\data-generate-loyalty-event\\signup.json");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        JSONArray jsonArray = null;
        if (message != null&&message.length() != 0) {
            jsonArray = JSONArray.parseArray(message);
        }
        if (jsonArray != null&&jsonArray.size() > 0) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                String tenantId = jsonObject.getString("tenantId");
                log.info("kafka producer message={}", jsonObject.toJSONString());
                kafkaTemplate.send("topic-mcloyalty-event-" + tenantId, "key", jsonObject.toJSONString());
            }
        }
        /*for (int i = 0; i < 1000; i++) {
            String message = "haha" + String.valueOf(i);
            log.info("kafka producer message={}", message);

            kafkaTemplate.send("test", "key", message);
        }*/
    }

    @Override
    public void sendMessage(String topic, Object key, Object msg) {
        ListenableFuture future = kafkaTemplate.send(topic, key.toString(), msg.toString());
        try {
            String str = future.get().toString();
            System.out.println(str);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendMessage(String topic, Object msg) {
        ListenableFuture future = kafkaTemplate.send(topic, msg.toString());
        try {
            String str = future.get().toString();
            System.out.println(str);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendMessage(String topic, int partition, Object key, Object msg) {
        kafkaTemplate.send(topic, partition, key.toString(), msg.toString());
    }
}
