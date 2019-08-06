package com.wj.queueenum;

/**
 * @author jun.wang
 * @title: QueueEnum
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 14:31
 */
public enum QueueEnum {

    TEST("test", "测试"),
    PROD("prod", "生产"),
    BIND("bind", ""),
    NAME_COUNT("name_count", "名字次数统计"),
    EVENT("event", "事件次数统计");

    private String queueName;
    private String desc;

    QueueEnum(String queueName, String desc) {
        this.queueName = queueName;
        this.desc = desc;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getDesc() {
        return desc;
    }
}
