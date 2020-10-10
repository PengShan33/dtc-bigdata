package com.dtc.java.SC.DP.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 资产大盘
 */
@Slf4j
public class DaPingZCDP extends RichSourceFunction<Tuple3<String, String, Integer>> {

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
//            String sql = "select count(*) as AllNum from asset a where a.room is not null and a.partitions is not null and a.box is not null";
//            String sql = "select ifnull(n.`name`,'其他') as name,ifnull(n.zc_name,'其他') as zc_name,n.num from (select * from (select m.zc_name,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
//                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) x left join asset_category y on x.pd = y.id) n group by n.`name`,n.zc_name";
           String sql = "select \n" +
                   "ifnull(c.name,'其他') as name,\n" +
                   "ifnull(b.name,'其他') as zc_name,\n" +
                   "count(1) as num\n" +
                   "from \n" +
                   "(\n" +
                   "select asset_id,parent_category_id, category_id from t_assalarm_asset\n" +
                   "where removed = 0\n" +
                   "group by asset_id,parent_category_id, category_id \n" +
                   ")a\n" +
                   "left join\n" +
                   "(\n" +
                   "select id,name from t_assalarm_asset_category where removed = 0\n" +
                   ")b\n" +
                   "on a.category_id = b.id\n" +
                   "left join\n" +
                   "(\n" +
                   "select id,name from t_assalarm_asset_category where removed = 0\n" +
                   ")c\n" +
                   "on a.parent_category_id = c.id\n" +
                   "group by c.name,b.name";


            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String zc_name = resultSet.getString("zc_name");
                int num = resultSet.getInt("num");
                ctx.collect(Tuple3.of(name, zc_name, num));
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
