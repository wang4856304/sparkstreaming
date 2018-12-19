package com.wj.dao;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

//@Repository
public class BaseDao {

    //@Autowired
    //@Qualifier("hBasePhoenixSqlSqlSessionTemplate")
    private SqlSessionTemplate hBasePhoenixSqlSqlSessionTemplate;

    //@Autowired
    //@Qualifier("masterSqlSessionTemplate")
    //private SqlSessionTemplate masterSqlSessionTemplate;

    public Integer insert(Map<String, Object> paramMap) {
        return hBasePhoenixSqlSqlSessionTemplate.insert("Test.insert", paramMap);
    }

    public List<Map<String, Object>> select(Map<String, Object> paramMap) {
        //System.out.println(hBasePhoenixSqlSqlSessionTemplate);
        //return masterSqlSessionTemplate.selectList("Test.selectMysql");
        return hBasePhoenixSqlSqlSessionTemplate.selectList("Test.select");
    }
}
