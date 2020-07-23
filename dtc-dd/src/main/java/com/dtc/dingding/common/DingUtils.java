package com.dtc.dingding.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.*;
import com.dingtalk.api.response.*;
import com.dtc.dingding.model.UserModel;
import com.dtc.dingding.model.UserIdModel;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.text.SimpleDateFormat;
import java.util.*;

public class DingUtils {

    public static String getAccess_Token(String appid, String appsecret, Properties props) {
        String accessToken = null;

        DefaultDingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_TOKEN).toString().trim());
        OapiGettokenRequest request = new OapiGettokenRequest();
        request.setAppkey(appid);
        request.setAppsecret(appsecret);
        request.setHttpMethod("GET");
        try {
            OapiGettokenResponse response = client.execute(request);
            accessToken = response.getAccessToken();
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return accessToken;
    }

    public static JSONArray getDepartmentList(String access_token, Properties props) {
        JSONArray department = null;

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
//        System.out.println("当前的系统时间为：" + sdf.format(new Date()));

        try {
            //获取部门列表
            DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_BUMEN_LIEBIAO).toString().trim());
            OapiDepartmentListRequest req = new OapiDepartmentListRequest();
            req.setHttpMethod("GET");
            OapiDepartmentListResponse rsp = client.execute(req, access_token);
            if (rsp.isSuccess()) {
                String body = rsp.getBody();
//                System.out.println("部门列表:"+body);
                JSONObject jsonObject = JSONObject.parseObject(body);
                department = (JSONArray) jsonObject.get("department");
            }
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return department;
    }

    /**
     * 获取各部门人员
     *
     * @param departmentID 部门ID
     * @param access_token token
     * @param props        配置参数
     */
    public static void getDepartmentUser(long departmentID, String access_token, Properties props) {

        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_BUMEN_USER).toString().trim());
        OapiUserSimplelistRequest request = new OapiUserSimplelistRequest();
        request.setDepartmentId(departmentID);
        request.setHttpMethod("GET");
        try {
            OapiUserSimplelistResponse response = client.execute(request, access_token);
            String body = response.getBody();
            JSONObject jsonObject = JSONObject.parseObject(body);
            if (!(jsonObject.getJSONArray("userlist").size() == 0)) {
                JSONArray userlist = jsonObject.getJSONArray("userlist");
                for (int i = 0; i < userlist.size(); i++) {
                    JSONObject job = userlist.getJSONObject(i);
                    String userID = job.get("userid").toString();
                    // 2.获取打卡信息: 3)获取人员详情信息
                    String userInfo = getUserInfo(userID, access_token, props);
//                    System.out.println("第四步 汇总用户详情+打卡信息:" + userInfo);

                    Gson gson = new Gson();
                    UserModel userModel = gson.fromJson(userInfo, UserModel.class);
                    SinkUtils.writeMysql("SC_KQ_USER", userModel, props);
                }
            }

        } catch (ApiException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取用户详情
     *
     * @param userID       用户唯一标识
     * @param access_token token
     * @param props        配置参数
     * @return
     */
    private static String getUserInfo(String userID, String access_token, Properties props) {
        String body;
        String jsonString = null;
        Map<String, String> userInfo = new HashMap<>();

        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_USER_INFO).toString().trim());
        OapiUserGetRequest request = new OapiUserGetRequest();
        request.setUserid(userID);
        request.setHttpMethod("GET");
        try {
            OapiUserGetResponse response = client.execute(request, access_token);
            // 2.获取打卡信息: 4)获取人员打卡数据
            Map<String, String> record = getRecord(userID, access_token, props);
            body = response.getBody();
//            System.out.println("第三步 获取员工详情:" + body);
            JSONObject jsonObject = JSONObject.parseObject(body);
            String department = jsonObject.get("department").toString();
            if (department.contains("[") && department.contains("]")) {
                String[] department1 = department.split("\\[")[1].split("]");
                record.put("department", department1[0]);
            } else {
                record.put("department", jsonObject.get("department").toString());
            }
            record.put("unionid", jsonObject.get("unionid").toString());
            record.put("openId", jsonObject.get("openId").toString());
            record.put("userid", jsonObject.get("userid").toString());
            record.put("mobile", jsonObject.get("mobile").toString());
            record.put("name", jsonObject.get("name").toString());
            jsonString = JSON.toJSONString(record);

            userInfo.put("userid", jsonObject.get("userid").toString());
            userInfo.put("name", jsonObject.get("name").toString());
            userInfo.put("mobile", jsonObject.get("mobile").toString());
            String userInfoStr = JSON.toJSONString(userInfo);
            Gson gson = new Gson();
            UserIdModel model = gson.fromJson(userInfoStr, UserIdModel.class);
            SinkUtils.writeMysql("SC_KQ_USERID", model, props);
        } catch (ApiException e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    /**
     * 获取用户打卡数据
     *
     * @param userID       用户唯一标识
     * @param access_token token
     * @param props        配置参数
     * @return
     * @throws ApiException
     */
    private static Map<String, String> getRecord(String userID, String access_token, Properties props) throws ApiException {
        DingTalkClient client = new DefaultDingTalkClient(props.get(PropertiesConstants.DD_DAKA).toString().trim());
//        OapiAttendanceListRecordRequest request = new OapiAttendanceListRecordRequest();
        OapiAttendanceListRequest request = new OapiAttendanceListRequest();
        Map<String, String> time = getTime();
        String startTime = time.get("starttime");
        String endTime = time.get("endtime");
        String start = timeStamp2Date(startTime, "yyyy-MM-dd HH:mm:ss");
        String end = timeStamp2Date(endTime, "yyyy-MM-dd HH:mm:ss");
        request.setWorkDateFrom(start);
        request.setWorkDateTo(end);
//        request.setWorkDateFrom("2020-07-16 00:00:00");
//        request.setWorkDateTo("2020-07-17 00:00:00");
        request.setUserIdList(Arrays.asList(userID));
        request.setOffset(0L);
        request.setLimit(10L);
        OapiAttendanceListResponse execute = client.execute(request, access_token);
        String body = execute.getBody();
//        System.out.println("第二步 获取用户打卡信息:" + body);

        JSONObject jsonObject = JSONObject.parseObject(body);
        if (jsonObject == null) {
            return new HashMap<>();
        }
        JSONArray recoder = (JSONArray) jsonObject.get("recordresult");
        Map<String, String> map = new HashMap();
        try {
            if (recoder.size() >= 2) {
                JSONObject job1 = recoder.getJSONObject(0);
                String checkType1 = job1.get("checkType").toString();
                if (checkType1.equals("OnDuty")) {
                    map.put(checkType1, job1.get("userCheckTime").toString());
                } else if (checkType1.equals("OffDuty")) {
                    map.put(checkType1, job1.get("userCheckTime").toString());
                }

                JSONObject job2 = recoder.getJSONObject(1);
                String checkType2 = job2.get("checkType").toString();
                if (checkType2.equals("OffDuty")) {
                    map.put(checkType2, job2.get("userCheckTime").toString());
                } else if (checkType2.equals("OnDuty")) {
                    map.put(checkType2, job2.get("userCheckTime").toString());
                }
            } else if (recoder.size() == 1) {
                JSONObject job = recoder.getJSONObject(0);
                String checkType = job.get("checkType").toString();
                if ("OnDuty".equals(checkType)) {
                    map.put("OnDuty", job.get("userCheckTime").toString());
                    map.put("OffDuty", "null");
                } else if ("OffDuty".equals(checkType)) {
                    map.put("OffDuty", job.get("userCheckTime").toString());
                    map.put("OnDuty", "null");
                }
            } else {
                map.put("OnDuty", "null");
                map.put("OffDuty", "null");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static Map<String, String> getTime() {
        Map<String, String> map = new HashMap<>();
        long timeStamp = System.currentTimeMillis();
        long yzero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset() - 24 * 60 * 60 * 1000;
        long zero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset();

        map.put("starttime", String.valueOf(yzero));
        map.put("endtime", String.valueOf(zero));
        return map;
    }

    public static String timeStamp2Date(String timeSeconds, String timeFormat) {
        if (timeSeconds == null || timeSeconds.isEmpty() || timeSeconds.equals("null")) {
            return "";
        }
        if (timeFormat == null || timeFormat.isEmpty()) {
            timeFormat = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        return sdf.format(new Date(Long.valueOf(timeSeconds)));
    }
}
