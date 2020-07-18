package com.dtc.java.SC.JKZL.sink;

import com.dtc.java.SC.common.PropertiesConstants;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSinkJFYC extends RichSinkFunction<Tuple5<String, String, String, String, Integer>> {
    private Properties properties;
    private Connection connection;
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private PreparedStatement preparedStatement;
    String str;

    public MysqlSinkJFYC(Properties prop) {
        this.properties = prop;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = properties.get(PropertiesConstants.MYSQL_USERNAME).toString();
        String passWord = properties.get(PropertiesConstants.MYSQL_PASSWORD).toString();
        String host = properties.get(PropertiesConstants.MYSQL_HOST).toString();
        String port = properties.get(PropertiesConstants.MYSQL_PORT).toString();
        String database = properties.get(PropertiesConstants.MYSQL_DATABASE).toString();

        String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl, userName
                , passWord);//写入mysql数据库
//        String sql = null;
        String sql = "replace into jfyc(riqi,asset_id,name,level_id,ip,num,js_time) values(?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        //insert sql在配置文件中
        super.open(parameters);
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
    public void invoke(Tuple5<String, String, String, String, Integer> value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            String asset_id = value.f0;
            String name = value.f1;
            String level_id = value.f2;
            String ip = value.f3;
            double num = value.f4;
            preparedStatement.setString(1, riqi);
            preparedStatement.setString(2, asset_id);
            preparedStatement.setString(3,name);
            preparedStatement.setString(4,level_id);
            preparedStatement.setString(5, ip);
            preparedStatement.setInt(6, (int) num);
            preparedStatement.setString(7,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

