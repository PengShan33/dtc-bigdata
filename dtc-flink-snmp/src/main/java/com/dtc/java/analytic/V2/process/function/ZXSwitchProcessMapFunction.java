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
 * 中兴交换机处理逻辑
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

                if (code.equals("102_103_103_109_109")) {
                    String new_value = in.getValue() + "_" + in.getTime();
                    if (mapSwitch.containsKey(in.getHost() + "." + in.getZbFourName())) {
                        String[] s = mapSwitch.get(in.getHost() + "." + in.getZbFourName()).split("_");
                        double lastValue = Double.parseDouble(s[0]);
                        long lastTime = Long.parseLong(s[1]);
                        double currentValue = Double.parseDouble(in.getValue());
                        long currentTime = Long.parseLong(in.getTime());
                        double result = 0;
                        try {
                            /**
                             * 交换机返回的为累加统计，所以需要中间等待后，再次获取，计算两次的差
                             *  返回的值为byte，需要先乘8，计算为bit然后再除1024，计算进位量
                             */
                            result = 8 * Math.abs((lastValue - currentValue)) / 1024 / ((currentTime - lastTime) / 1000);
                        } catch (ArithmeticException exc) {
                            log.error("交换机端口出速率计算时，时间差为0.", exc);
                        }
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "102_101_103_104_105", in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                        mapSwitch.put(in.getHost() + "." + in.getZbFourName(), new_value);
                        continue;
                    } else {
                        mapSwitch.put(in.getHost() + "." + in.getZbFourName(), new_value);
                        continue;
                    }
                }
                if (code.equals("102_103_103_108_108")) {
                    String new_value = in.getValue() + "_" + in.getTime();
                    if (mapSwitch.containsKey(in.getHost() + "." + in.getZbFourName())) {
                        String[] s = mapSwitch.get(in.getHost() + "." + in.getZbFourName()).split("_");
                        double lastValue = Double.parseDouble(s[0]);
                        long lastTime = Long.parseLong(s[1]);
                        double currentValue = Double.parseDouble(in.getValue());
                        long currentTime = Long.parseLong(in.getTime());
                        double result;
                        try {
                            /**
                             * 交换机返回的为累加统计，所以需要中间等待后，再次获取，计算两次的差
                             *  返回的值为byte，需要先乘8，计算为bit然后再除1024，计算进位量
                             */
                            result = 8 * Math.abs((lastValue - currentValue)) / 1024 / ((currentTime - lastTime) / 1000);
                        } catch (ArithmeticException exc) {
                            log.error("交换机端口入速率计算时，时间差为0.", exc);
                            continue;
                        }
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "102_101_103_105_106", in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                        mapSwitch.put(in.getHost() + "." + in.getZbFourName(), new_value);
                    } else {
                        mapSwitch.put(in.getHost() + "." + in.getZbFourName(), new_value);
                    }
                }
            }
        }
    }
}


