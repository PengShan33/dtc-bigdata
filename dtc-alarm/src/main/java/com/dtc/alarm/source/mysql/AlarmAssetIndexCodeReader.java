package com.dtc.alarm.source.mysql;

import com.dtc.alarm.domain.AlarmAssetIndexCode;
import com.dtc.alarm.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author
 */
@Slf4j
public class AlarmAssetIndexCodeReader extends RichSourceFunction<AlarmAssetIndexCode> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = JdbcUtil.getConnection(parameterTool);
        if (null != connection) {
            String sql = "select code,ip,subcode from t_assalarm_asset_index_code;";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<AlarmAssetIndexCode> sourceContext) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                AlarmAssetIndexCode indexCode = new AlarmAssetIndexCode();
                indexCode.setCode(resultSet.getString("code"));
                indexCode.setIp(resultSet.getString("ip"));
                indexCode.setSubCode(resultSet.getString("subcode"));
                sourceContext.collect(indexCode);
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
