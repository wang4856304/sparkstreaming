package com.wj.kafka.producer;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/
public interface KafkaProducerService {

    void sendMessage();
    void sendMessage(String topic, Object key, Object msg);
    void sendMessage(String topic, Object msg);
    void sendMessage(String topic, int partition, Object key, Object msg);
}
