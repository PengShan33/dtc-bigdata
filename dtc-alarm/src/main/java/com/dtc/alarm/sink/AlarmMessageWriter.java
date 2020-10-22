package com.dtc.alarm.sink;

import com.dtc.alarm.constant.PropertiesConstants;
import com.dtc.alarm.domain.AlertStruct;
import com.dtc.alarm.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author
 */
@Slf4j
public class AlarmMessageWriter extends RichSinkFunction<AlertStruct> {


    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = JdbcUtil.getConnection(parameterTool);
        preparedStatement = connection.prepareStatement(parameterTool.get(PropertiesConstants.SQL));
        super.open(parameters);
    }

    @Override
    public void invoke(AlertStruct value, Context context) throws Exception {
        //TODO 告警信息
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
