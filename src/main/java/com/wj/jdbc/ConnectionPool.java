package com.wj.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.*;

/**
 * @author jun.wang
 * @title: ConnectionPool
 * @projectName ownerpro
 * @description: TODO
 * @date 2019/6/20 15:06
 */
public class ConnectionPool {

    private int defaultPoolSize = 10;
    private String jdbcUrl;
    private String userName;
    private String password;
    private String driverName;
    private int maxPoolSize = 100;
    private long timeout = 0;

    private LinkedBlockingQueue<Connection> connectionList = new LinkedBlockingQueue<>();

    public ConnectionPool(String jdbcUrl, String userName, String password, String driverName, int defaultPoolSize) {
        this.defaultPoolSize = defaultPoolSize;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
        this.driverName = driverName;
        init();
    }

    private void init() {
        if (defaultPoolSize > maxPoolSize) {
            throw new  RuntimeException("defaultPoolSize is greater than maxPoolSize");
        }
        try {
            for (int i = 0; i < defaultPoolSize; i++) {
                Connection connection = DriverManager.getConnection(jdbcUrl, userName, password);
                connectionList.add(connection);
            }
        }
        catch (Exception e) {
            throw new  RuntimeException("init pool fail", e);
        }
    }

    public synchronized Connection getConnection() {
        if (connectionList.size() == 0) {
            throw new RuntimeException("there is no valid connection");
        }
        try {
            Connection connection = connectionList.poll(timeout, TimeUnit.SECONDS);
            if (connection == null) {
                throw new RuntimeException("get connection timeout");
            }
            return connection;
        }
        catch (Exception e) {
            throw new RuntimeException("get connection error", e);
        }
    }

    public boolean returnConnection(Connection connection) {
        if (connection != null) {
            connectionList.add(connection);
            return true;
        }
        return false;
    }
}
