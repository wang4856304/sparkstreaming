package com.wj.spark;

import java.io.Serializable;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/
public interface SparkService extends Serializable {
    void runSpark() throws Exception;
    void runSparkKafka() throws Exception;
    String getDataBySql(String sql);
}
