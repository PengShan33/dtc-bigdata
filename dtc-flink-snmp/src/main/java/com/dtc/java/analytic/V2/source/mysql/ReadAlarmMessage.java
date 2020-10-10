package com.dtc.java.analytic.V2.source.mysql;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class ReadAlarmMessage extends RichSourceFunction<Tuple9<String, String, String, String, Double, String, String, String, String>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        String driver = "com.mysql.jdbc.Driver";
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST).trim();
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT).trim();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE).trim();
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME).trim();
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD).trim();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {

//            String sql = "select c.asset_id,d.ipv4,b.strategy_id,b.trigger_kind,b.trigger_name,b.number,b.unit,b.code,a.is_enable,a.alarm_level,a.up_time,d.code as asset_code,d.name,e.id\n" +
//                    "from\n" +
//                    "(\n" +
//                    "select\n" +
//                    "*\n" +
//                    "from\n" +
//                    "alarm_strategy\n" +
//                    "where is_enable = 1\n" +
//                    ") a\n" +
//                    "left join\n" +
//                    "(\n" +
//                    "select * from strategy_trigger where code is not null and code != \"\" and (comparator = \">\" or comparator = \">=\")\n" +
//                    ")b\n" +
//                    "on a.id = b.strategy_id\n" +
//                    "left join strategy_asset_mapping c\n" +
//                    "on b.strategy_id = c.strategy_id\n" +
//                    "left join asset d\n" +
//                    "on c.asset_id = d.id\n" +
//                    "left join asset_indice e\n" +
//                    "on b.code = e.code";

            String sql = "select c.asset_id,d.ipv4,b.strategy_id,b.trigger_kind,b.trigger_name,b.number,b.unit,b.code,a.is_enable,a.alarm_level,a.up_time,d.code as asset_code,d.name,e.id\n" +
                    "from\n" +
                    "(\n" +
                    "select\n" +
                    "*\n" +
                    "from\n" +
                    "alarm_strategy\n" +
                    "where is_enable = 1\n" +
                    ") a\n" +
                    "left join\n" +
                    "(\n" +
                    "select * from strategy_trigger where code is not null and code != \"\" and (comparator = \">\" or comparator = \">=\")\n" +
                    ")b\n" +
                    "on a.id = b.strategy_id\n" +
                    "left join strategy_asset_mapping c\n" +
                    "on b.strategy_id = c.strategy_id\n" +
                    "\n" +
                    "left join \n" +
                    "(\n" +
                    "select\n" +
                    "asset_id,\n" +
                    "max(case when item_code = 'asset_name' then item_value end) as 'name',\n" +
                    "max(case when item_code = 'asset_code' then item_value end) as 'code',\n" +
                    "max(case when item_code = 'ip_address' then item_value end) as 'ipv4'\n" +
                    "from t_assalarm_asset\n" +
                    "where removed = 0 and (item_code = 'asset_name'  or item_code = 'asset_code'  or item_code = 'ip_address')\n" +
                    "group by asset_id\n" +
                    ")d\n" +
                    "on c.asset_id = d.asset_id\n" +
                    "\n" +
                    "left join asset_indice e\n" +
                    "on b.code = e.code";



            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple9<String, String, String, String, Double, String, String, String, String>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if ("1".equals(resultSet.getString("is_enable"))) {
                    String asset_id = resultSet.getString("asset_id") + "|" + resultSet.getString("id") + "|" + resultSet.getString("strategy_id");
                    String ipv4 = resultSet.getString("ipv4");
                    String strategy_kind = resultSet.getString("trigger_kind");
                    String triger_name = resultSet.getString("trigger_name");
                    double number = resultSet.getDouble("number");
                    String code = resultSet.getString("code");
                    String alarm_level = resultSet.getString("alarm_level");
                    String asset_code = resultSet.getString("asset_code");
                    String name = resultSet.getString("name");
                    ctx.collect(Tuple9.of(asset_id, ipv4, strategy_kind, triger_name, number, code, alarm_level, asset_code, name));
                }
            }
            Thread.sleep(1000 * 6);
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
