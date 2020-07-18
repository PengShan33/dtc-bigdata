package com.dtc.dingding.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiProcessinstanceGetRequest;
import com.dingtalk.api.request.OapiProcessinstanceListidsRequest;
import com.dingtalk.api.response.OapiProcessinstanceGetResponse;
import com.dingtalk.api.response.OapiProcessinstanceListidsResponse;
import com.dtc.dingding.common.PropertiesConstants;
import com.dtc.dingding.model.JBModel;
import com.dtc.dingding.model.QJModel;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.dtc.dingding.common.DingUtils.getTime;
import static com.dtc.dingding.common.SinkUtils.writeMysql;

public class JiaBan {
    public static void getJiaBan(String access_token, Properties props) throws ApiException {
        // 5.获取加班数据: 1)获取加班审批列表
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/listids");
        OapiProcessinstanceListidsRequest req = new OapiProcessinstanceListidsRequest();
        req.setProcessCode(props.get(PropertiesConstants.DD_JIABAN).toString());
        Map<String, String> time = getTime();
        Long starttime = Long.parseLong(time.get("starttime"));
        Long endtime = Long.parseLong(time.get("endtime"));
//        Long starttime = Long.parseLong("1594569600000");
//        Long endtime = Long.parseLong("1594656000000");
        req.setStartTime(starttime);
        req.setEndTime(endtime);
        OapiProcessinstanceListidsResponse response = client.execute(req, access_token);
        String str = response.getBody();
//        System.out.println("第一步 获取加班列表:" + str);

        JSONObject jsonObject = JSONObject.parseObject(str);
        JSONArray result = jsonObject.getJSONObject("result").getJSONArray("list");
        // 5.获取加班数据: 2)获取加班审批详情,并提取所需数据
        for (int i = 0; i < result.size(); i++) {
            String str1 = result.getString(i);
            DingTalkClient client1 = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/get");
            OapiProcessinstanceGetRequest request = new OapiProcessinstanceGetRequest();
            request.setProcessInstanceId(str1);
            OapiProcessinstanceGetResponse response1 = client1.execute(request, access_token);
            String body = response1.getBody();
//            System.out.println("第二步 获取加班详情:" + body);
            Map<String, String> map = new HashMap<>();
            JSONObject jsonObject1 = JSONObject.parseObject(body);
            String status = jsonObject1.getJSONObject("process_instance").getString("status");
            String task_result = jsonObject1.getJSONObject("process_instance").getString("result");
            if (task_result.equals("agree")) {
                map.put("approve_status","已审批");
            } else {
                map.put("approve_status","未审批");
            }

            JSONArray operation_records = jsonObject1.getJSONObject("process_instance").getJSONArray("operation_records");
            for (int j = 0; j < operation_records.size(); j++) {
                JSONObject jsonObject2 = operation_records.getJSONObject(j);
                String operation_type = jsonObject2.getString("operation_type");
                if ("START_PROCESS_INSTANCE".equals(operation_type)) {
                    String userid = jsonObject2.getString("userid");
                    map.put("userid", userid);
                    JSONArray form_component_values = jsonObject1.getJSONObject("process_instance").getJSONArray("form_component_values");
                    for (int m = 0; m < form_component_values.size(); m++) {
                        JSONObject jsonObject3 = form_component_values.getJSONObject(m);
                        if ("加班时长".equals(jsonObject3.getString("name"))) {
                            String duration = jsonObject3.getString("value");
                            if (duration.contains("小时")) {
                                String duration1 = duration.replace("小时", "");
                                map.put("jiaban_time", duration1);
                            } else {
                                map.put("jiaban_time", duration);
                            }
                        }
                        if ("开始时间-结束时间".equals(jsonObject3.getString("id"))) {
                            String value = jsonObject3.getString("value");
                            if (value.startsWith("[") && value.endsWith("]")) {
                                String[] split = value.split("\\[")[1].split("]");
                                String start_time = split[0].split(",")[0].replace("\"", "");
                                String end_time = split[0].split(",")[1].replace("\"", "");
                                map.put("start_time", start_time);
                                map.put("end_time", end_time);
                            }
                        }
                        if ("加班原因".equals(jsonObject3.getString("id"))) {
                            String value = jsonObject3.getString("value");
                            map.put("reason", value);
                        }
                    }
                }
            }
            JSONArray tasks = jsonObject1.getJSONObject("process_instance").getJSONArray("tasks");
            if (tasks.size() > 1) {
                JSONObject tasksJSONObject1 = tasks.getJSONObject(0);
                String finish_time1 = tasksJSONObject1.getString("finish_time");
                String userid1 = tasksJSONObject1.getString("userid");
                map.put("first_approve_time",finish_time1);
                map.put("first_approver_userid",userid1);

                JSONObject tasksJSONObject2 = tasks.getJSONObject(1);
                String finish_time2 = tasksJSONObject2.getString("finish_time");
                String userid2 = tasksJSONObject2.getString("userid");
                map.put("second_approve_time",finish_time2);
                map.put("second_approver_userid",userid2);
            } else if (tasks.size()==1) {
                JSONObject tasksJSONObject1 = tasks.getJSONObject(0);
                String finish_time1 = tasksJSONObject1.getString("finish_time");
                String userid1 = tasksJSONObject1.getString("userid");
                map.put("first_approve_time",finish_time1);
                map.put("first_approver_userid",userid1);
                map.put("second_approve_time","null");
                map.put("second_approver_userid","null");
            }

            String jsonString = JSON.toJSONString(map);
            Gson gson = new Gson();
            JBModel model = gson.fromJson(jsonString, JBModel.class);
            writeMysql("SC_KQ_JB", model, props);
        }
    }
}
