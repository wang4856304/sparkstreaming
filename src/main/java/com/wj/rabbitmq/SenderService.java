package com.wj.rabbitmq;

/**
 * @author jun.wang
 * @title: SenderService
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 15:05
 */
public interface SenderService {

    void send(String queueName, Object msg);
    void send(String exchangeName, String routeKey, Object msg);
}
