package com.dtc.alarm.util;

import com.dtc.alarm.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author
 */
@Slf4j
public class JdbcUtil {

    public static Connection getConnection(ParameterTool parameterTool) {

        String driver = parameterTool.get(PropertiesConstants.MYSQL_DRIVER).trim();
        String url = parameterTool.get(PropertiesConstants.MYSQL_URL).trim();
        String user = parameterTool.get(PropertiesConstants.MYSQL_USERNAME).trim();
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD).trim();
        return getConnection(driver, url, user, password);
    }

    public static Connection getConnection(String driver, String url, String user, String password) {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            log.error("get mysql connection exception.", e);
        }
        return connection;
    }
}
