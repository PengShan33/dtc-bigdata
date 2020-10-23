package com.dtc.java.analytic.V2.sink.mysql;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySqlSinkUtils {

    public static void subCodeSinktoMysql(DataStruct element, ParameterTool parameters) throws SQLException {
        String userName = parameters.get(PropertiesConstants.MYSQL_USERNAME);
        String passWord = parameters.get(PropertiesConstants.MYSQL_PASSWORD);
        String host = parameters.get(PropertiesConstants.MYSQL_HOST);
        String port = parameters.get(PropertiesConstants.MYSQL_PORT);
        String database = parameters.get(PropertiesConstants.MYSQL_DATABASE);
        String subcode_table_sql = parameters.get(PropertiesConstants.MYSQL_SUBCODE_TABLE);

        String Url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        String JDBCDriver = "com.mysql.jdbc.Driver";
        Connection con = null;
        try {
            Class.forName(JDBCDriver);
            con = DriverManager.getConnection(Url, userName, passWord);
            PreparedStatement pst = con.prepareStatement(subcode_table_sql);
            pst.setString(1, element.getZbFourName());
            pst.setString(2, element.getZbLastCode());
            pst.setString(3, element.getHost());
            pst.executeUpdate();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                con.close();
            }

        }
    }

}
