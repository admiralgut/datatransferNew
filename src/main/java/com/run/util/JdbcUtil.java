package com.run.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class JdbcUtil {
    private static Logger LOG = LoggerFactory.getLogger(JdbcUtil.class);
    private static Connection conn;
    private static String dbURL;
    private static String dbUser;
    private static String dbPwd;

    static {
        dbURL = ConfigUtil.getProperty("db.url");
        dbUser = ConfigUtil.getProperty("db.username");
        dbPwd = ConfigUtil.getProperty("db.password");
    }

    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //LOG.info("success to connect to database.");
            return DriverManager.getConnection(dbURL, dbUser, dbPwd);
        } catch (Exception e) {
            LOG.error("数据库连接错误：{}", e);
            e.printStackTrace();
            throw new RuntimeException("创建数据库连接错误");
        }
    }

    public static int executeUpdate(String sql) {
        Connection conn = getConnection();
        int ret = 0;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            ret = stmt.executeUpdate(sql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            closeConnection(conn);
        }
        return ret;
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void truncateTalbe(String tableName) {
        String sql = "truncate table " + tableName;
        executeUpdate(sql);
    }

    public static void main(String[] args) {
        Connection conn = getConnection();
        executeUpdate("update test set c2='bbb' where c1 = 111");
        closeConnection(conn);
    }
}
