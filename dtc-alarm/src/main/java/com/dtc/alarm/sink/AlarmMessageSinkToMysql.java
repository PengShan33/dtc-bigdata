package com.dtc.alarm.sink;

import com.dtc.alarm.constant.PropertiesConstants;
import com.dtc.alarm.domain.AlterStruct;
import com.dtc.alarm.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author
 */
@Slf4j
public class AlarmMessageSinkToMysql extends RichSinkFunction<AlterStruct> {


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
    public void invoke(AlterStruct value, Context context) throws Exception {
        //TODO 告警信息
        try {
            //code,name,asset_id,indice_val,level_id,description,time_occur,rule,indice_id,strategy_id,con_asset_id,con_alarm
            String code = UUIDGenerator.generateUserCode();
            String Unique_id = value.getUniqueId();
            boolean contains = Unique_id.contains("|");
            String asset_id = null;
            String index_id = null;
            String strategy_id = null;
            if (contains) {
                String[] split = Unique_id.split("\\|");
                asset_id = split[0];
                index_id = split[1];
                strategy_id = split[2];
            }

            String timeOccur = timeStamp2Date(value.getOccurTime(), "");
            preparedStatement.setString(1, code);
            preparedStatement.setString(2, value.getTriggerName());
            preparedStatement.setString(3, asset_id);
            preparedStatement.setString(4, value.getValue());
            preparedStatement.setString(5, value.getLevel());
            preparedStatement.setString(6, value.getDescription());
            preparedStatement.setString(7, timeOccur);
            preparedStatement.setString(8, value.getRule());
            preparedStatement.setString(9, index_id);
            preparedStatement.setString(10, strategy_id);
            preparedStatement.setString(11, value.getCon_asset_id());
            preparedStatement.setString(12, value.getCon_alarm());
            preparedStatement.executeUpdate();
            System.out.println(value.getDescription() + " 打印告警数据写入到msyql中,策略id是 " + strategy_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    public String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }
}

class UUIDGenerator {

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random rng = new SecureRandom();

    private static char randomChar() {
        return ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
    }

    public static String uuid(int length, int spacing, char spacerChar) {
        StringBuilder sb = new StringBuilder();
        int spacer = 0;
        while (length > 0) {
            if (spacer == spacing) {
                sb.append(spacerChar);
                spacer = 0;
            }
            length--;
            spacer++;
            sb.append(randomChar());
        }
        return sb.toString();
    }

    public static String generateUserCode() {
        return uuid(6, 10, ' ');
    }

}
