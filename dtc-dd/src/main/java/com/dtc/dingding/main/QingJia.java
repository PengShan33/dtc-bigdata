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
import com.dtc.dingding.model.QJModel;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.dtc.dingding.common.DingUtils.getTime;
import static com.dtc.dingding.common.SinkUtils.writeMysql;

public class QingJia {
    public static void getQingJia(String access_token, Properties props) throws ApiException, ParseException {
        // 3.获取请假数据: 1)获取请假审批列表
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/listids");
        OapiProcessinstanceListidsRequest req = new OapiProcessinstanceListidsRequest();
        req.setProcessCode(props.get(PropertiesConstants.DD_QINGJIA).toString());
        Map<String, String> time = getTime();
        Long stattime = Long.parseLong(time.get("starttime"));
        Long endtime = Long.parseLong(time.get("endtime"));
        req.setStartTime(stattime);
        req.setEndTime(endtime);
        OapiProcessinstanceListidsResponse response = client.execute(req, access_token);
        String str = response.getBody();
//        System.out.println("第一步 获取请假审批列表:" + str);

        JSONObject jsonObject = JSONObject.parseObject(str);
        JSONArray result = jsonObject.getJSONObject("result").getJSONArray("list");
        // 3.获取请假数据: 2)获取请假审批详情,并提取所需数据
        for (int i = 0; i < result.size(); i++) {
            String str1 = result.getString(i);
            DingTalkClient client1 = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/get");
            OapiProcessinstanceGetRequest request = new OapiProcessinstanceGetRequest();
            request.setProcessInstanceId(str1);
            OapiProcessinstanceGetResponse response1 = client1.execute(request, access_token);
            String body = response1.getBody();
//            System.out.println("第二步 获取请假详情:"+body);
            JSONObject jsonObject1 = JSONObject.parseObject(body);
            String status = jsonObject1.getJSONObject("process_instance").getString("status");
            String task_result = jsonObject1.getJSONObject("process_instance").getString("result");
            Map<String, String> map = new HashMap<>();
            if ("COMPLETED".equals(status) && "agree".equals(task_result)) {
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
                            if ("请假天数".equals(jsonObject3.getString("name"))) {
                                map.put("qingjia_time", jsonObject3.getString("value"));
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
                        }
                    }
                }
                String jsonString = JSON.toJSONString(map);
                Gson gson = new Gson();
                QJModel model = gson.fromJson(jsonString, QJModel.class);
                writeMysql("SC_KQ_QJ", model, props);
            }
        }
    }
}
