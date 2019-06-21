package com.wj.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContextUtil implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextUtil.applicationContext = applicationContext;
    }

    public static ApplicationContext getContext(){
        return applicationContext;
    }

    public static <T> T getBean(String beanName) {
        if (applicationContext.containsBean(beanName)) {
            return (T)applicationContext.getBean(beanName);
        }
        return null;
    }
    public  static <T> T getBean(String beanName, Class<T> clazz) {
        if (applicationContext.containsBean(beanName)) {
            return applicationContext.getBean(beanName, clazz);
        }
        else {
            throw new RuntimeException("can not find bean, bean name is " + beanName);
        }
    }

}
