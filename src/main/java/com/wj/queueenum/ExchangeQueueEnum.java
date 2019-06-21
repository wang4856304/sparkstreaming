package com.wj.queueenum;

import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

/**
 * @author jun.wang
 * @title: ExchangeQueueEnum
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 17:51
 */
public enum ExchangeQueueEnum {

    TEST("test", "exchange-test", Queue.class, Exchange.class, "bind.test", "bind-test"),
    PROD("bind", "exchange-test", Queue.class, Exchange.class, "bind.#", "bind-prod");
    private String queueName;
    private String exchangeName;
    private Class<?> queueType;
    private Class<?> exchangeType;
    private String bindKey;
    private String bindName;

    ExchangeQueueEnum(String queueName, String exchangeName, Class<?> queueType, Class<?> exchangeType, String bindKey, String bindName) {
        this.queueName = queueName;
        this.exchangeName = exchangeName;
        this.queueType = queueType;
        this.exchangeType = exchangeType;
        this.bindKey = bindKey;
        this.bindName = bindName;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public Class<?> getQueueType() {
        return queueType;
    }

    public Class<?> getExchangeType() {
        return exchangeType;
    }

    public String getBindKey() {
        return bindKey;
    }

    public String getBindName() {
        return bindName;
    }
}
