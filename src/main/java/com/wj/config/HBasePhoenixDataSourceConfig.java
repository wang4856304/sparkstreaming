package com.wj.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import javax.sql.DataSource;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

//@Configuration
@ConfigurationProperties(prefix = "hbase.phoenix")
@EnableTransactionManagement
public class HBasePhoenixDataSourceConfig {

    private String url;
    private String driverClassName;

    @Bean
    public DataSource hBasePhoenixDatasource() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(url);
        druidDataSource.setDriverClassName(driverClassName);
        druidDataSource.setDefaultAutoCommit(true);
        return druidDataSource;
    }

    public JdbcTemplate phoenixTemplate(@Qualifier("hBasePhoenixDatasource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate;
    }

    /*@Bean
    public SqlSessionFactory hBasePhoenixSqlSessionFactory(@Qualifier("hBasePhoenixDatasource") DataSource dataSource) throws Exception {
        final SqlSessionFactoryBean sessionFactory = new SqlSessionFactoryBean();
        sessionFactory.setDataSource(dataSource);
        sessionFactory.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath:mapper/*.xml"));
        //sessionFactory.setConfigLocation(new PathMatchingResourcePatternResolver().getResource("classpath:mybatis-config.xml"));
        return sessionFactory.getObject();
    }

    @Bean
    public SqlSessionTemplate hBasePhoenixSqlSqlSessionTemplate(@Qualifier("hBasePhoenixSqlSessionFactory") SqlSessionFactory sqlSessionFactory) {
        SqlSessionTemplate sqlSessionTemplate = new SqlSessionTemplate(sqlSessionFactory);
        return sqlSessionTemplate;
    }

    @Bean
    public DataSourceTransactionManager hBasePhoenixTransactionManager(@Qualifier("hBasePhoenixDatasource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }*/

}
