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
import com.dtc.dingding.model.BKModel;
import com.google.gson.Gson;
import com.taobao.api.ApiException;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.dtc.dingding.common.DingUtils.getTime;
import static com.dtc.dingding.common.SinkUtils.writeMysql;

public class BuKa {
    public static void getBuKa(String access_token, Properties props) throws ApiException, ParseException {
        // 4.获取补卡数据: 1)获取补卡审批列表
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/listids");
        OapiProcessinstanceListidsRequest req = new OapiProcessinstanceListidsRequest();
        req.setProcessCode(props.get(PropertiesConstants.DD_BUKA).toString());
        Map<String, String> time = getTime();
        Long starttime = Long.parseLong(time.get("starttime"));
        Long endtime = Long.parseLong(time.get("endtime"));

        req.setStartTime(starttime);
        req.setEndTime(endtime);
        OapiProcessinstanceListidsResponse response = client.execute(req, access_token);
        String str = response.getBody();
//        System.out.println("第一步 获取补卡审批列表:" + str);

        JSONObject jsonObject = JSONObject.parseObject(str);
        JSONArray result = jsonObject.getJSONObject("result").getJSONArray("list");
        // 4.获取补卡数据: 2)获取补卡审批详情,并提取所需数据
        for (int i = 0; i < result.size(); i++) {
            String str1 = result.getString(i);
            DingTalkClient client1 = new DefaultDingTalkClient("https://oapi.dingtalk.com/topapi/processinstance/get");
            OapiProcessinstanceGetRequest request = new OapiProcessinstanceGetRequest();
            request.setProcessInstanceId(str1);
            OapiProcessinstanceGetResponse response1 = client1.execute(request, access_token);
            String body = response1.getBody();
//            System.out.println("第二步 获取补卡详情:"+body);
            Map<String, String> map = new HashMap<>();
            JSONObject jsonObject1 = JSONObject.parseObject(body);
            String status = jsonObject1.getJSONObject("process_instance").getString("status");
            String task_result = jsonObject1.getJSONObject("process_instance").getString("result");
            String userID = jsonObject1.getJSONObject("process_instance").getString("originator_userid");
            map.put("userid", userID);
            if ("COMPLETED".equals(status) && "agree".equals(task_result)) {
                JSONArray operation_records = jsonObject1.getJSONObject("process_instance").getJSONArray("form_component_values");
                for (int j = 0; j < operation_records.size(); j++) {
                    JSONObject jsonObject2 = operation_records.getJSONObject(j);
                    String operation_type = jsonObject2.getString("name");
                    if ("repairCheckTime".equals(operation_type)) {
                        String buKaTime = jsonObject2.getString("value");
                        map.put("buka_time", buKaTime);
                    }
                }
                String jsonString = JSON.toJSONString(map);
                Gson gson = new Gson();
                BKModel model = gson.fromJson(jsonString, BKModel.class);
                writeMysql("SC_KQ_BK", model,props);
            }
        }
    }
}
