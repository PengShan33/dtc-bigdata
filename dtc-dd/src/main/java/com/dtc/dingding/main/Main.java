package com.dtc.dingding.main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtc.dingding.common.DingUtils;
import com.dtc.dingding.common.PropertiesConstants;
import com.taobao.api.ApiException;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class Main {
    // 获取token第三方用户唯一凭证
    public static String appid = "dingmobls5odfginbbhs";
    // 获取token第三方用户唯一凭证密钥
    public static String appsecret = "d4H-UjqGtqHF5Dnu09wvgwRgxYqwgV1WlFAl-yh0OmrmWXl5ZpQn6f5q0ttr0cfI";

    public static void main(String[] args) throws ParseException {
        // 获取配置文件
        InputStream ips = Main.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME);
        Properties props = new Properties();
        try {
            props.load(ips);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                ips.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // 1.获取token
        String access_token = DingUtils.getAccess_Token(appid, appsecret, props);
//        String access_token = "139d3e08503f3755aad9dc8b29abb0fb";
        System.out.println("token:" + access_token);

        /*DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String classDateString = "2020-08-01";
        Date classDate = format.parse(classDateString);
        Date nextDay = classDate;
        System.out.println(classDate);

        Calendar c = Calendar.getInstance();
        c.setTime(classDate);

        for (int i = 0; i < 8; i++) {

            c.setTime(nextDay);
            long stime = nextDay.getTime();
            System.out.println(format.format(nextDay) + ":" + stime);

            c.add(Calendar.DAY_OF_MONTH, 1);
            nextDay = c.getTime();
            long etime = nextDay.getTime();
            System.out.println(format.format(nextDay) + ":" + etime);

            System.out.println("================================");


            // 2.获取打卡数据: 1)获取部门列表
            JSONArray department = DingUtils.getDepartmentList(access_token, props);
//        System.out.println("第一步 获取部门列表:" + department);
            *//*for (int j = 0; j < department.size(); j++) {
                JSONObject job = department.getJSONObject(j);
                if ("true".equals(String.valueOf(job.get("createDeptGroup")))) {
                    String id = job.get("id").toString();
                    // 获取打卡信息: 2)获取各部门人员列表
                    DingUtils.getDepartmentUser(Long.parseLong(id), access_token, props, stime, etime);
                }
            }*//*
            try {
                // 3.获取请假数据
//                QingJia.getQingJia(access_token, props,stime, etime);
                // 4.获取补卡数据
//                BuKa.getBuKa(access_token, props,stime, etime);
                // 5.获取加班数据
                JiaBan.getJiaBan(access_token, props,stime, etime);
            } catch (ApiException | ParseException e) {
                e.printStackTrace();
            }
        }*/

        // 2.获取打卡数据: 1)获取部门列表
        JSONArray department = DingUtils.getDepartmentList(access_token, props);
//        System.out.println("第一步 获取部门列表:" + department);
        for (int i = 0; i < department.size(); i++) {
            JSONObject job = department.getJSONObject(i);
            if ("true".equals(String.valueOf(job.get("createDeptGroup")))) {
                String id = job.get("id").toString();
                // 获取打卡信息: 2)获取各部门人员列表
                DingUtils.getDepartmentUser(Long.parseLong(id), access_token, props);
            }
        }
        try {
            // 3.获取请假数据
            QingJia.getQingJia(access_token,props);
            // 4.获取补卡数据
            BuKa.getBuKa(access_token,props);
            // 5.获取加班数据
            JiaBan.getJiaBan(access_token,props);
        } catch (ApiException | ParseException e) {
            e.printStackTrace();
        }
    }
}
