package com.wj.config;

import com.wj.queueenum.ExchangeEnum;
import com.wj.queueenum.ExchangeQueueEnum;
import com.wj.queueenum.QueueEnum;
import com.wj.utils.SpringContextUtil;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.lang.reflect.Constructor;

/**
 * @author jun.wang
 * @title: QueueDynamicConfiguration
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/5/30 14:49
 */

@Configuration
public class QueueDynamicConfiguration {

    @Autowired
    private ApplicationContext applicationContext;

    /**
     * 创建队列
     * void 返回类型启动报异常
     * @return
     */
    @Bean
    public Runnable dynamicQueueConfiguration()
    {
        ConfigurableApplicationContext context = (ConfigurableApplicationContext)applicationContext;
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)context.getBeanFactory();

        QueueEnum[] queueEnums = QueueEnum.values();
        for (QueueEnum queueEnum: queueEnums) {
            String name = queueEnum.getQueueName();
            Queue queue = new Queue(name);
            //BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.rootBeanDefinition(Queue.class);
            /**
             * 设置属性
             */
            //beanDefinitionBuilder.addPropertyValue("name", name);
            /**
             * 注册到spring容器中
             */
            //beanFactory.registerBeanDefinition(name, beanDefinitionBuilder.getBeanDefinition());
            beanFactory.registerSingleton(name, queue);
        }
        return null;
    }

    /**
     * 创建exchange
     * void 返回类型启动报异常
     * @return
     */
    @Bean
    public Runnable dynamicExchangeConfiguration() throws Exception
    {
        ConfigurableApplicationContext context = (ConfigurableApplicationContext)applicationContext;
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)context.getBeanFactory();

        ExchangeEnum[] exchangeEnums = ExchangeEnum.values();
        for (ExchangeEnum exchangeEnum: exchangeEnums) {
            String name = exchangeEnum.getExchangeName();
            Class<?> exchangeType = exchangeEnum.getExchangeType();
            Constructor<?> con=exchangeType.getConstructor(String.class);
            Object object = con.newInstance(name);
            beanFactory.registerSingleton(name, object);
        }
        return null;
    }

    /**
     * 绑定exchange与queue
     * void 返回类型启动报异常
     * @return
     */
    @Bean
    public Runnable dynamicExchangeQueueConfiguration() throws Exception
    {
        ConfigurableApplicationContext context = (ConfigurableApplicationContext)applicationContext;
        DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory)context.getBeanFactory();

        ExchangeQueueEnum[] ExchangeQueueEnums = ExchangeQueueEnum.values();
        for (ExchangeQueueEnum exchangeQueueEnum: ExchangeQueueEnums) {
            String exchangeName = exchangeQueueEnum.getExchangeName();
            String queueName = exchangeQueueEnum.getQueueName();
            Class<?> exchangeType = exchangeQueueEnum.getExchangeType();
            Class<?> queueType = exchangeQueueEnum.getQueueType();
            String bindKey = exchangeQueueEnum.getBindKey();
            String bindName = exchangeQueueEnum.getBindName();
            Exchange exchange = SpringContextUtil.getBean(exchangeName, exchangeType);
            Queue queue = SpringContextUtil.getBean(queueName, queueType);
            Binding binding = BindingBuilder.bind(queue).to(exchange).with(bindKey).noargs();
            beanFactory.registerSingleton(bindName, binding);
        }
        return null;
    }
}
