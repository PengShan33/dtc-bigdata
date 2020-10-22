package com.dtc.alarm.function.map;

import com.dtc.alarm.constant.PropertiesConstants;
import com.dtc.alarm.enums.FunctionEnum;
import com.dtc.alarm.util.OpenTSDBUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.opentsdb.client.HttpClientImpl;
import org.opentsdb.client.util.Aggregator;

import java.io.IOException;
import java.util.Map;

/**
 * @author
 */
public class OpenTSDBFlatMapFunction extends RichFlatMapFunction<Tuple5<String, String, Integer, Integer, String>,
        Tuple6<String, String, Integer, Integer, String, Map<String, Object>>> {

    private HttpClientImpl httpClient;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String openTSDBUrl = parameterTool.get(PropertiesConstants.OPENTSDB_URL);
        httpClient = new HttpClientImpl(openTSDBUrl);

    }

    @Override
    public void flatMap(Tuple5<String, String, Integer, Integer, String> tuple5,
                        Collector<Tuple6<String, String, Integer, Integer, String, Map<String, Object>>> collector) throws Exception {
        Tuple6<String, String, Integer, Integer, String, Map<String, Object>> tuple6 = getResult(tuple5);
        collector.collect(tuple6);
    }

    private Tuple6<String, String, Integer, Integer, String, Map<String, Object>> getResult(
            Tuple5<String, String, Integer, Integer, String> tuple5) throws IOException {
        //code,ipv4,target,pastTimeSecond,subcode
        String code = tuple5.f0;
        String ip = tuple5.f1;
        String func = getFunc(tuple5.f2);
        int pastTimeSecond = tuple5.f3;
        String subCode = tuple5.f4;
        String metric = "";
        if (StringUtils.isNotEmpty(subCode)) {
            metric = code + "-" + subCode + "-" + ip;
        } else {
            metric = code + "-" + ip;
        }

        Map<String, Object> map = OpenTSDBUtil.queryOpenTSDB(httpClient, metric, ip, func, pastTimeSecond);
        return Tuple6.of(code, ip, tuple5.f2, pastTimeSecond, tuple5.f4, map);
    }


    private String getFunc(int index) {

        String func = "";
        if (StringUtils.isEmpty(String.valueOf(index))) {
            func = Aggregator.avg.toString();
        } else if (index == FunctionEnum.MAX.getOrder()) {
            func = Aggregator.max.toString();
        } else if (index == FunctionEnum.MIN.getOrder()) {
            func = Aggregator.min.toString();
        } else if (index == FunctionEnum.AVG.getOrder()) {
            func = Aggregator.avg.toString();
        }

        return func;
    }
}
