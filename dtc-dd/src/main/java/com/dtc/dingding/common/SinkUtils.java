package com.dtc.dingding.common;

import com.dtc.dingding.model.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;


public class SinkUtils {
    public static void writeMysql(String tableName, SuperModel model, Properties props) {
        Connection con = null;
        long time = System.currentTimeMillis();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String jisuan_riqi = sdf.format(new Date(Long.valueOf(time)));

        UserModel userModel = null;
        QJModel jqModel = null;
        BKModel bkModel = null;
        JBModel jbModel =null;

        if (model instanceof UserModel) {
            userModel = (UserModel) model;
            String daka_riqi = getBeforeTime();

            String sql = "replace into " + tableName + "(" + "user_id," + "name," + "mobile," + "department," + "OnDuty," + "OnBaseTime," + "OffDuty," + "OffBaseTime," + "daka_riqi," + "jisuan_riqi" + ") values(?,?,?,?,?,?,?,?,?,?)";//数据库操作语句（插入）
            //  String sql1 = "insert ignore into USER (user_id,unionid,openIdmobile,department,OnDuty,OffDuty,riqi,name,jisuan_riqi,OnResult,OffResult) values(?,?,?,?,?,?,?,?,?,?,?,?)";//数据库操作语句（插入）
            try {
                con = MySQLUtils.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, userModel.getUserid());
                pst.setString(2, userModel.getName());
                pst.setString(3, userModel.getMobile());
                pst.setString(4, userModel.getDepartment());
                pst.setString(5, userModel.getOnDuty());
                pst.setString(6, "09:05");
                pst.setString(7, userModel.getOffDuty());
                pst.setString(8, "17:30");
                pst.setString(9,daka_riqi);
                pst.setString(10, jisuan_riqi);
                pst.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } else if (model instanceof QJModel) {
            jqModel = (QJModel) model;
            try {
                String sql = "replace into " + tableName + "(" + "user_id," + "duration," + "start_time," + "end_time," + "jisuan_riqi" + ") values(?,?,?,?,?)";//数据库操作语句（插入）
                con = MySQLUtils.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, jqModel.getUserid());
                pst.setString(2, jqModel.getTime());
                pst.setString(3, jqModel.getStarttime());
                pst.setString(4, jqModel.getEndtime());
                pst.setString(5, jisuan_riqi);
                pst.executeUpdate();//解释在下
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if (model instanceof BKModel) {
            bkModel = (BKModel) model;
            try {
                String sql = "replace into " + tableName + "(" + "user_id," + "buka_time," + "jisuan_riqi" + ") values(?,?,?)";//数据库操作语句（插入）
                con = MySQLUtils.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, bkModel.getUserid());
                pst.setString(2, bkModel.getTime());
                pst.setString(3, jisuan_riqi);
                pst.executeUpdate();//解释在下
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else if (model instanceof JBModel) {
            jbModel = (JBModel) model;
            try {
                String sql = "replace into " + tableName + "(" + "user_id," + "duration," + "start_time," + "end_time," + "reason," + "approve_status,"  + "first_approver_userid," +"first_approve_time," +"second_approver_userid," + "second_approve_time," + "jisuan_riqi" + ") values(?,?,?,?,?,?,?,?,?,?,?)";//数据库操作语句（插入）
                con = MySQLUtils.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, jbModel.getUserid());
                pst.setString(2, jbModel.getTime());
                pst.setString(3, jbModel.getStarttime());
                pst.setString(4, jbModel.getEndtime());
                pst.setString(5, jbModel.getReason());
                pst.setString(6, jbModel.getStatus());
                pst.setString(7, jbModel.getFirstApprover());
                pst.setString(8, jbModel.getFirstApproveTime());
                pst.setString(9, jbModel.getSecondApprover());
                pst.setString(10, jbModel.getSecondApproveTime());
                pst.setString(11, jisuan_riqi);
                pst.executeUpdate();//解释在下
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (con != null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static String getBeforeTime() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        Date start = calendar.getTime();
        String beforeOneDay= format.format(start);//前一天
        return beforeOneDay;
    }
}
