package com.dtc.java.analytic.V1.alter;


import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class GetAlarmNotifyData extends RichSourceFunction<Map<String, String>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
            String sql = "select * from dtc_test";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> map = new HashMap<>();
        Tuple4<String, String, Short, String> test = null;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
//                if (1 == resultSet.getInt("id")) {
                if ("true".equals(resultSet.getString("used"))) {
                    String unique_id = resultSet.getString("unique_id");
                    String ip = resultSet.getString("ip");
                    String code = resultSet.getString("code");
                    String alarm = resultSet.getString("alarm");
                    String key = unique_id + ":" + ip;
                    String result = unique_id + ":" + code + ":" + alarm;
                    map.put(ip, result);
                }

            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);

            ctx.collect(map);
            map.clear();
//            Thread.sleep(2000 * 60);
            Thread.sleep(20 * 60);
        }

    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }
}
