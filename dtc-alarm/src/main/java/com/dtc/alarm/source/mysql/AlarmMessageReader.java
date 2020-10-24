package com.dtc.alarm.source.mysql;

import com.dtc.alarm.domain.AlarmMessage;
import com.dtc.alarm.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 加载告警规则
 *
 * @author
 */
@Slf4j
public class AlarmMessageReader extends RichSourceFunction<AlarmMessage> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    /**
     * 是否开启告警规则 0:停止；1:启动
     */
    private final String enableFlag = "1";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = JdbcUtil.getConnection(parameterTool);
        if (connection != null) {
            String sql = "select \n" +
                    "tttt.asset_id,\n" +
                    "tttt.con_asset_id,\n" +
                    "tttt.con_alarm,\n" +
                    "tttt.strategy_id,\n" +
                    "tttt.trigger_kind,tttt.trigger_name,tttt.comparator,\n" +
                    "tttt.number,tttt.unit,tttt.code,tttt.is_enable,\n" +
                    "tttt.alarm_level,tttt.up_time,\n" +
                    "tttt.past_time,\n" +
                    "tttt.time_unit,\n" +
                    "tttt.target,\n" +
                    "tttt.past_time_second,\n" +
                    "d.ipv4,\n" +
                    "d.code as asset_code,d.name,e.id\n" +
                    "from \n" +
                    "(\n" +
                    "select \n" +
                    "mmm.*\n" +
                    "from\n" +
                    "(\n" +
                    "select \n" +
                    "*\n" +
                    "from\n" +
                    "(\n" +
                    "select\n" +
                    "c.asset_id,\n" +
                    "concat(c.asset_id,',',b.code,b.comparator,b.number,b.unit) as con_asset_id,\n" +
                    "concat(b.trigger_name,b.comparator,b.number,b.unit) as con_alarm,\n" +
                    "b.strategy_id,\n" +
                    "b.trigger_kind,b.trigger_name,b.comparator,\n" +
                    "b.number,b.unit,b.code,a.is_enable,\n" +
                    "a.alarm_level,a.up_time,\n" +
                    "b.past_time_second,\n" +
                    "b.past_time,\n" +
                    "b.time_unit,\n" +
                    "b.target\n" +
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
                    "select * from strategy_trigger \n" +
                    "where code is not null and code != \"\"\n" +
                    "and comparator is not null\n" +
                    "and number is not null\n" +
                    "and unit is not null\n" +
                    "and past_time is not null\n" +
                    "and time_unit is not null\n" +
                    "and target is not null \n" +
                    ")b\n" +
                    "on a.id = b.strategy_id\n" +
                    "left join strategy_asset_mapping c\n" +
                    "on b.strategy_id = c.strategy_id\n" +
                    ")t1\n" +
                    "group by con_asset_id,past_time_second\n" +
                    ")mmm\n" +
                    "left join \n" +
                    "(\n" +
                    "    select\n" +
                    "con_asset_id,\n" +
                    "min(past_time_second) as past_time_second\n" +
                    "from\n" +
                    "(\n" +
                    "select\n" +
                    "c.asset_id,\n" +
                    "concat(c.asset_id,',',b.code,b.comparator,b.number,b.unit) as con_asset_id,\n" +
                    "b.strategy_id,\n" +
                    "b.trigger_kind,b.trigger_name,b.comparator,\n" +
                    "b.number,b.unit,b.code,a.is_enable,\n" +
                    "a.alarm_level,a.up_time,\n" +
                    "b.past_time_second,\n" +
                    "b.past_time,\n" +
                    "b.time_unit,\n" +
                    "b.target\n" +
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
                    "select * from strategy_trigger where code is not null and code != \"\"\n" +
                    ")b\n" +
                    "on a.id = b.strategy_id\n" +
                    "left join strategy_asset_mapping c\n" +
                    "on b.strategy_id = c.strategy_id\n" +
                    ")t\n" +
                    "group by con_asset_id\n" +
                    ") nnn\n" +
                    "on mmm.con_asset_id = nnn.con_asset_id and mmm.past_time_second=nnn.past_time_second\n" +
                    "where nnn.past_time_second is not null\n" +
                    ")tttt\n" +
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
                    "on tttt.asset_id = d.asset_id\n" +
                    "left join asset_indice e\n" +
                    "on tttt.code = e.code";

            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<AlarmMessage> context) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if (enableFlag.equals(resultSet.getString("is_enable"))) {
                    AlarmMessage alarmMessage = new AlarmMessage();
                    alarmMessage.setAssetId(resultSet.getString("asset_id") + "|" + resultSet.getString("id") + "|" + resultSet.getString("strategy_id"));
                    alarmMessage.setConAssetId(resultSet.getString("con_asset_id"));
                    alarmMessage.setConAlarm(resultSet.getString("con_alarm"));
                    alarmMessage.setStrategyId(resultSet.getString("strategy_id"));
                    alarmMessage.setIpv4(resultSet.getString("ipv4"));
                    alarmMessage.setTriggerKind(resultSet.getString("trigger_kind"));
                    alarmMessage.setTriggerName(resultSet.getString("trigger_name"));
                    alarmMessage.setComparator(resultSet.getString("comparator"));
                    alarmMessage.setNumber(resultSet.getDouble("number"));
                    alarmMessage.setUnit(resultSet.getString("unit"));
                    alarmMessage.setCode(resultSet.getString("code"));
                    alarmMessage.setAlarmLevel(resultSet.getString("alarm_level"));
                    alarmMessage.setUpTime(resultSet.getString("up_time"));
                    alarmMessage.setAssetCode(resultSet.getString("asset_code"));
                    alarmMessage.setName(resultSet.getString("name"));
                    alarmMessage.setPastTimeSecond(resultSet.getInt("past_time_second"));
                    alarmMessage.setTarget(resultSet.getInt("target"));
                    alarmMessage.setConAlarm(resultSet.getString("con_alarm"));
                    alarmMessage.setPastTime(resultSet.getInt("past_time"));
                    alarmMessage.setTimeUnit(resultSet.getString("time_unit"));

                    context.collect(alarmMessage);
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
            log.error("flink job cancel exception.", e);
        }
        isRunning = false;
    }
}
