package com.dtc.java.SC.DP.sink;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink_DP extends RichSinkFunction<Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        // 加载JDBC驱动
        connection = MySQLUtil.getConnection(parameterTool);
        String sql = "replace into SC_DP_SJDP(riqi,wo_num,bg_num,w_leve_1,w_leve_2,w_leve_3,w_leve_4,w_level_all,wb_num,qy_num,jk_num,js_time) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
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
        super.close();
    }

    @Override
    public void invoke(Tuple11<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
        //(标志，工单，变更，等级1，2，3，4，总和,维保，废弃，健康)
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            //(标志，工单，变更，等级1，2，3，4，总和,维保，废弃，健康)
            preparedStatement.setString(1, riqi);
            preparedStatement.setInt(2,value.f1);
            preparedStatement.setInt(3,value.f2);
            preparedStatement.setInt(4,value.f3);
            preparedStatement.setInt(5,value.f4);
            preparedStatement.setInt(6,value.f5);
            preparedStatement.setInt(7,value.f6);
            preparedStatement.setInt(8,value.f7);
            preparedStatement.setInt(9,value.f8);
            preparedStatement.setInt(10,value.f9);
            preparedStatement.setInt(11,value.f10);
            preparedStatement.setString(12,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

