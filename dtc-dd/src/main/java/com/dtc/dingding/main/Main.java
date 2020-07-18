package com.dtc.dingding.main;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtc.dingding.common.DingUtils;
import com.dtc.dingding.common.PropertiesConstants;
import com.taobao.api.ApiException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    // 获取token第三方用户唯一凭证
    public static String appid = "dingmobls5odfginbbhs";
    // 获取token第三方用户唯一凭证密钥
    public static String appsecret = "d4H-UjqGtqHF5Dnu09wvgwRgxYqwgV1WlFAl-yh0OmrmWXl5ZpQn6f5q0ttr0cfI";

    public static void main(String[] args) {
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
//        String access_token="96a36fbc1be33f19b9d4262ea43b763f";
//        System.out.println("token:" + access_token);

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
        } catch (ApiException e) {
            e.printStackTrace();
        }
    }
}
