package com.dtc.java.SC.JGPF;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 各机房各区域各机柜设备总数
 */
@Slf4j
public class ReadDataFM extends RichSourceFunction<Order> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;
    private long interval_time;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        interval_time = Long.parseLong(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        connection = MySQLUtil.getConnection(parameterTool);

        if (connection != null) {
            String sql = "select a.room,a.partitions,a.box,count(*) as num from asset a group by a.room,a.partitions,a.box having a.room is not null and a.partitions is not null and a.box is not null";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Order> ctx) throws Exception {
        Tuple4<String, String, Short, String> test = null;
        Integer id = 0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                id = resultSet.getInt("num");
                String room = resultSet.getString("room");
                Order order = new Order(room, id);
                ctx.collect(order);
            }
            Thread.sleep(interval_time);
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
