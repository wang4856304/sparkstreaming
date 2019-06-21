package com.wj.rabbitmq.impl;

import com.wj.rabbitmq.SenderService;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author jun.wang
 * @title: SenderServiceImpl
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 15:06
 */

@Service
public class SenderServiceImpl implements SenderService {
    @Autowired
    private AmqpTemplate rabbitTemplate;

    @Override
    public void send(String queueName, Object msg) {
        rabbitTemplate.convertAndSend(queueName, msg);
    }

    @Override
    public void send(String exchangeName, String routeKey, Object msg) {
        rabbitTemplate.convertAndSend(exchangeName, routeKey, msg);
    }
}
