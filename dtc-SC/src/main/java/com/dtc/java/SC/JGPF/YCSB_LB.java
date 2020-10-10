package com.dtc.java.SC.JGPF;


import com.dtc.java.SC.JKZL.model.YCSB_LB_Model;
import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 各机房未关闭告警数
 */
@Slf4j
public class YCSB_LB extends RichSourceFunction<YCSB_LB_Model> {

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
//           String sql = "select a.asset_id,a.level_id,count(*) as num,b.`name`,b.ipv4 as ip,b.room,b.partitions,b.box from alarm a left join asset b on b.id =a.asset_id group by a.asset_id,a.level_id having b.room is not null and b.`partitions` is not null and b.box is not null";
            String sql = "select\n" +
                    "b.asset_id,\n" +
                    "a.level_id,\n" +
                    "count(1) as num,\n" +
                    "b.asset_name as name,\n" +
                    "b.ip_address as ip,\n" +
                    "b.computer_room_name as room,\n" +
                    "b.partitions,\n" +
                    "b.box\n" +
                    "from\n" +
                    "(select * from alarm where status != 2)a\n" +
                    "left join \n" +
                    "(\n" +
                    "select \n" +
                    "asset_id,\n" +
                    "computer_room_name,\n" +
                    "name as partitions,\n" +
                    "cabinet_row,\n" +
                    "cabinet_number,\n" +
                    "ip_address,\n" +
                    "asset_name,\n" +
                    "box\n" +
                    "from\n" +
                    "(\n" +
                    "select\n" +
                    "asset_id,\n" +
                    "max(case when item_code = 'computer_room_name' then item_value end) as 'computer_room_name',\n" +
                    "max(case when item_code = 'partition' then item_value end) as 'partitions',\n" +
                    "max(case when item_code = 'cabinet_row' then item_value end) as 'cabinet_row',\n" +
                    "max(case when item_code = 'cabinet_number' then item_value end)  as 'cabinet_number',\n" +
                    "max(case when item_code = 'ip_address' then item_value end)  as 'ip_address',\n" +
                    "max(case when item_code = 'asset_name' then item_value end)  as 'asset_name',\n" +
                    "concat(max(case when item_code = 'cabinet_row' then item_value end),\"排;\",max(case when item_code = 'cabinet_number' then item_value end),\"号机柜\") as box\n" +
                    "from t_assalarm_asset\n" +
                    "where removed = 0 and (item_code = 'computer_room_name'  or item_code = 'partition'  or item_code = 'cabinet_row' or item_code = 'cabinet_number' or item_code = 'ip_address')\n" +
                    "group by asset_id\n" +
                    ")m\n" +
                    "left join \n" +
                    "(\n" +
                    "select code,name from \n" +
                    "t_dic_item where type_code = 'partition'\n" +
                    ")n\n" +
                    "on m.partitions = n.code\n" +
                    ")b\n" +
                    "on a.asset_id = b.asset_id\n" +
                    "where b.asset_id is not null\n" +
                    "and computer_room_name != ''  \n" +
                    "and partitions != ''  \n" +
                    "and cabinet_row != ''  \n" +
                    "and cabinet_number != ''\n" +
                    "group by \n" +
                    "b.asset_id,\n" +
                    "a.level_id,\n" +
                    "b.asset_name,\n" +
                    "b.ip_address,\n" +
                    "b.computer_room_name,\n" +
                    "b.partitions,\n" +
                    "b.box";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<YCSB_LB_Model> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String asset_id = resultSet.getString("asset_id").trim();
                String level_id = resultSet.getString("level_id").trim();
                String name = resultSet.getString("name").trim();
                String ip = resultSet.getString("ip").trim();
                int num = resultSet.getInt("num");
                String room = resultSet.getString("room");
                String partitions = resultSet.getString("partitions");
                String box = resultSet.getString("box");
                YCSB_LB_Model ycsb_lb_model = new YCSB_LB_Model(asset_id,level_id,num,name,ip,room,partitions,box);
                ctx.collect(ycsb_lb_model);
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
