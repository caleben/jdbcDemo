package com.synway.bigdata.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcManager {
    private static final Logger log = LoggerFactory.getLogger(JdbcManager.class);
    private Connection connection;
    private Statement stmt = null;
    private DataSource dataSource;
    private String type;
    private final String CONF = "global.properties";

    public JdbcManager initJdbc(String type) {
        this.type = type;
        dataSource = setupDateSource();
        try {
            connection = dataSource.getConnection();
            stmt = connection.createStatement();
            log.info(String.format("成功获取%s连接！", type));
        } catch (SQLException e) {
            log.warn("adapter异常",e.getMessage());
            e.printStackTrace();
        }
        return this;
    }

    private Configuration getConf() {
        Configuration conf = new PropertiesConfiguration();
        try {
            conf = new PropertiesConfiguration(JdbcManager.class.getResource("/" + CONF));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
        return conf;
    }

    private DataSource setupDateSource() {
        BasicDataSource ds = new BasicDataSource();
        try {
            Configuration prop = getConf();
            String driver = null, url = null, username = null, paswd = null;
            if (type.equalsIgnoreCase("hive")) {
                driver = prop.getString("db.hive.driver");
                url = prop.getString("db.hive.url");
                username = prop.getString("db.hive.user");
                paswd = prop.getString("db.hive.password");
                ds.setValidationQuery("select 1");
            } else if (type.equalsIgnoreCase("oracle")) {
                driver = prop.getString("db.oracle.driver");
                url = prop.getString("db.oracle.url");
                username = prop.getString("db.oracle.user");
                paswd = prop.getString("db.oracle.password");
                ds.setValidationQuery("select 1 from dual");
            } else {
                throw new RuntimeException("初始化DataSource失败: 参数只能为hive或oracle!");
            }
            log.info(String.format("获取%s数据库连接信息：\ndriver-->\t%s\nurl----->\t%s\nusername->\t%s",
                    type, driver, url, username));
            ds.setDriverClassName(driver);
            ds.setUrl(url);
            ds.setUsername(username);
            ds.setPassword(paswd);
            ds.setConnectionProperties("connectTimeout=3000;socketTimeout=60000");
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            throw new RuntimeException("当前元数据库配置错误", e);
        }
        return ds;
    }

    private void showSql(String sql) {
        log.info(sql);
    }

    /**
     * 用于查询语句：select/show/desc...
     *
     * @param sql an SQL statement to be sent to the database
     * @return ResultSet:查询sql的结果集。
     * @throws SQLException
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        return stmt.executeQuery(sql);
    }

    /**
     * 用于DML操作：insert/delete/update
     * 用于DDL操作：create/alter/drop
     *
     * @param sql an SQL statement to be sent to the database
     * @return DML操作返回操作作成功的行数；DDL操作返回 0
     * @throws SQLException
     */
    public int executeUpdate(String sql) throws SQLException {
        return stmt.executeUpdate(sql);
    }

    /**
     * 用于更新插入数据
     * @param sql 插入的sql语句
     * @param params 传递多个参数
     * @return 插入成功true,否则false
     */
    public boolean insert(String sql, Object...params) {
        try {
            showSql(sql);
            int i = new QueryRunner(dataSource).update(sql, params);
            return i == 1;
        } catch (SQLException e) {
            log.error("更新错误sql：" + sql + "\n" + e.getMessage());
        }
        return false;
    }

    /**
     * 释放资源
     */
    public void destroy() {
        if (stmt != null) {
            try {
                stmt.close();
                log.info("stmt关闭成功");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
                log.info("当前" +type + "连接关闭成功");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void setHiveActiQueryMode() {
        try {
            stmt.execute("set hive.compactor.initiator.on = true");
            stmt.execute("set hive.compactor.worker.threads = 1");
            stmt.execute("set hive.support.concurrency = true");
            stmt.execute("set hive.enforce.bucketing = true");
            stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict");
            stmt.execute("set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }
}
