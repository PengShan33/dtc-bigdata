package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2019-12-26
 * 华三交换机处理逻辑
 *
 * @author :hao.li
 */
@Slf4j
public class ZXSwitchProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
    /**
     * 此处的map<code(in.f2.in.f3),value_time>
     * */
    Map<String, String> mapSwitch = new HashMap<>();
    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> collector) throws Exception {
        for (DataStruct in : iterable) {
            String code = in.getZbFourName();
            //判断是否是数据
            boolean strResult = in.getValue().matches("(^-?[0-9][0-9]*(.[0-9]+)?)$");
            if (!strResult) {
                log.info("Value is not number of string!");
            } else {
                if (code.equals("102_103_101_101_101") || code.equals("102_103_101_102_102") ||
                        code.equals("102_103_102_103_103") || code.equals("102_103_103_105_105") ||
                        code.equals("102_103_103_106_106") || code.equals("102_103_103_107_107")
                        || code.equals("102_103_103_108_108")|| code.equals("102_103_103_109_109")
                        || code.equals("102_103_103_110_110")) {
                    /**
                     *交换机cpu使用率/内存总量/内存利用率/端口入方向错误报文数/端口出方向错误报文数/端口的工作状态
                     * 以交换机cpu使用率数据格式为例说明如下：
                     * (system_name,ip,ZN_Name,ZB_code,time,value)
                     * (102_101,ip,102_101_101_101_101,101.1.0,time,value)
                     *其中ZB_code为例：机器ip,1.0表示机框1，0板卡，表示机框1，板卡0的cpu使用率
                     */
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(),in.getNameCN(), in.getNameEN(), in.getTime(),in.getValue()));
                }
            }
        }
    }
}


