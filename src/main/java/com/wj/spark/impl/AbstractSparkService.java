package com.wj.spark.impl;

import com.wj.spark.SparkService;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.util.StringUtils;

import java.util.Map;
import java.util.Set;

/**
 * @author jun.wang
 * @title: AbstratSparkService
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/6/27 10:56
 */
public abstract class AbstractSparkService implements SparkService {

    /**
     *
     * @param hostName 主机地址
     * @param appName  task名称
     * @param checkPointDir 持久化目录
     * @param batchDuration 批处理时间间隔
     * @param prop 配置属性
     * @return
     */
    JavaStreamingContext createSparkContext(String hostName, String appName, String checkPointDir, Duration batchDuration,
                                            Map<String, String> prop) {
        SparkConf sparkConf = new SparkConf().setMaster(hostName).setAppName(appName);
        if (prop != null&&prop.size() > 0) {
            Set<Map.Entry<String, String>> set = prop.entrySet();
            for (Map.Entry<String, String> entry: set) {
                sparkConf.set(entry.getKey(), entry.getValue());
            }
        }
        JavaStreamingContext jsc  = new JavaStreamingContext(sparkConf, batchDuration);//创建context上下文
        /*if (!StringUtils.isEmpty(checkPointDir)) {
            jsc.checkpoint(checkPointDir);//spark 持久化容错目录设置
            //driver服务重启时，恢复原有流程或数据
            JavaStreamingContext backupJsc = JavaStreamingContext.getOrCreate(checkPointDir, ()->jsc);
            return backupJsc;
        }*/
        return jsc;
    }
}
