package com.synway.bigdata.service;

import com.synway.bigdata.util.JdbcManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class WriteToOracle {
    private static final Logger log = LoggerFactory.getLogger(WriteToOracle.class);
    private static JdbcManager hive = new JdbcManager().initJdbc("hive");
    private static JdbcManager oracle = new JdbcManager().initJdbc("oracle");

    private static String getYesterdayDate() {
        String str;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        str = df.format(new Date());
        int yesterday = Integer.valueOf(str) - 1;
        return String.valueOf(yesterday);
    }

    private void writeToFemaleTable(String date) {
        String sourcesql_xj = "select * from shehuang.nb_app_shhz_xj where dt= '" + date + "'";
        String sql = "insert into PROSTITUTION_FEMALE(MD_ID,CHINESE_NAME,MSISDN,ID_NUM,SEXCODE, CAPTURE_TIME) values (?, ?, ?, ?, ?, ?)";
        ResultSet resultSet_xj = null;
        try {
            hive.setHiveActiQueryMode();
            resultSet_xj = hive.executeQuery(sourcesql_xj);
            long startTime = System.currentTimeMillis();
            long count = insertToPersonDatas(resultSet_xj, sql);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            log.info(String.format("成功插入%d条记录到表female中，用时%d秒。", count, duration / 1000));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet_xj != null) resultSet_xj.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void writeToMaleTable(String date) {
        String sourcesql_pk = "select * from shehuang.nb_app_shhz_pk where dt= '" + date + "'";
        String sql = "insert into PROSTITUTION_MALE(MD_ID,CHINESE_NAME,MSISDN,ID_NUM,SEXCODE,CAPTURE_TIME) values (?, ?, ?, ?, ?, ?)";
        ResultSet resultSet_pk = null;
        try {
            hive.setHiveActiQueryMode();
            resultSet_pk = hive.executeQuery(sourcesql_pk);
            long startTime = System.currentTimeMillis();
            long count = insertToPersonDatas(resultSet_pk, sql);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            log.info(String.format("成功插入%d条记录到表male中，用时%d秒。", count, duration / 1000));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet_pk != null) resultSet_pk.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void writToScoreTable(String date) {
        String sourcesql_pk = "select * from shehuang.nb_app_xiaojie_score_weekly where dt= '" + date + "'";
        String sql = "insert into PROSTITUTION_SCORE(MD_ID,msisdn_1,counttimes_score,countdays_score," +
                "newconnections_score,callnum_score,phonenum_score,avg_duration_score,total_score) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        ResultSet resultSet_pk = null;
        try {
            hive.setHiveActiQueryMode();
            resultSet_pk = hive.executeQuery(sourcesql_pk);
            long startTime = System.currentTimeMillis();
            long count = insertToScoreDatas(resultSet_pk, sql);
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            log.info(String.format("成功插入%d条记录到表score中，用时%d秒。", count, duration / 1000));
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet_pk != null) resultSet_pk.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private long insertToScoreDatas(ResultSet rs, String sql) throws SQLException {
        long count = 0;
        while (rs.next()) {
            boolean b = oracle.insert(sql, rs.getString(1), rs.getString(2),
                    rs.getFloat(3), rs.getFloat(4), rs.getFloat(5),
                    rs.getFloat(6), rs.getFloat(7), rs.getFloat(8),
                    rs.getFloat(9));
            if (b) ++count;
        }
        return count;
    }

    private long insertToPersonDatas(ResultSet rs, String sql) throws SQLException {
        long count = 0;
        while (rs.next()) {
            boolean b = oracle.insert(sql, rs.getString(1), rs.getString(2),
                    rs.getString(3), rs.getString(4), rs.getString(5),
                    rs.getLong(6));
            if (b) ++count;
        }
        return count;
    }

    private void writeToAlltable(String date) {
        writeToFemaleTable(date);
        writeToMaleTable(date);
        writToScoreTable(date);
    }

    private static boolean isNum(String input) {
        try {
            Integer.parseInt(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static void main(String[] args) {
        try {
            WriteToOracle wto = new WriteToOracle();
            Properties date = new Properties();
            if (args.length == 1 && args[0].length() == 8 && isNum(args[0])) {
                date.setProperty("date", args[0]);
                log.info("输入的分区为dt=" + args[0]);
            } else if (args.length == 0) {
                log.info("没有参数，按默认分区dt=" + getYesterdayDate());
            } else {
                log.error("只支持一个参数，参数格式应为:yyyymmdd,实际输入为:" + args[0]);
                throw new RuntimeException("只支持一个参数，参数格式应为:yyyymmdd,实际输入为:" + args[0]);
            }
            wto.writeToAlltable(date.getProperty("date", getYesterdayDate()));
        } finally {
            hive.destroy();
            oracle.destroy();
        }
    }
}
