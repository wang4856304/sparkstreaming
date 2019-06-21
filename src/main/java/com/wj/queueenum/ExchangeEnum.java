package com.wj.queueenum;

import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.TopicExchange;

/**
 * @author jun.wang
 * @title: ExchangeEnum
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 17:26
 */
public enum  ExchangeEnum {

    TEST("exchange-test", TopicExchange.class,"测试"),
    PROD("exchange-prod", FanoutExchange.class, "生产");

    private String exchangeName;
    private Class exchangeType;
    private String desc;

    ExchangeEnum(String exchangeName, Class exchangeType, String desc) {
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.desc = desc;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public Class getExchangeType() {
        return exchangeType;
    }

    public String getDesc() {
        return desc;
    }
}
