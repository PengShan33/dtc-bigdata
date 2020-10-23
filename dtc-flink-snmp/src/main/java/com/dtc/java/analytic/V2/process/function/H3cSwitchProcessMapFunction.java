package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.enums.H3cSwitchCodeTypeEnum;
import com.dtc.java.analytic.V2.sink.mysql.MySqlSinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * 华三交换机处理逻辑
 *
 */
@Slf4j
public class H3cSwitchProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
    Map<String, String> portSpeedMap = new HashMap<>();
    Map<String, String> portsOutSpeedMap = new HashMap<>();
    Map<String, String> portsInSpeedMap = new HashMap<>();
    Map<String, String> cardNumMap = new HashMap<>();
    Map<String, String> portNumMap = new HashMap<>();

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> out) throws Exception {
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        for (DataStruct element : iterable) {
            boolean result = element.getValue().matches("(^-?[0-9][0-9]*(.[0-9]+)?)$");
            if (!result) {
                log.info("value is not number of string!" + element.getValue());
            } else {
                //cpu使用率,直接使用
                if (H3cSwitchCodeTypeEnum.CPU_PERCET.getCode().equals(element.getZbFourName())) {
                    String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                    String value = element.getValue();
                    cardNumMap.put(key, value);
                    //板卡编码写入后端mysql库
                    MySqlSinkUtils.subCodeSinktoMysql(element,parameters);

                    out.collect(element);
                    continue;
                }

                //内存使用率,直接使用
                if (H3cSwitchCodeTypeEnum.MEM_USED_PERCET.getCode().equals(element.getZbFourName())) {
                    out.collect(element);
                    continue;
                }

                //端口工作状态,直接使用
                if (H3cSwitchCodeTypeEnum.PORT_STATUS.getCode().equals(element.getZbFourName())) {
                    String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                    String value = element.getValue();
                    portNumMap.put(key, value);
                    //端口编码写入后端mysql库
                    MySqlSinkUtils.subCodeSinktoMysql(element,parameters);

                    out.collect(element);
                    continue;
                }

                //各端口出速率计算
                if (H3cSwitchCodeTypeEnum.IF_OUTOCTETS.getCode().equals(element.getZbFourName())) {
                    String new_value = element.getValue() + "_" + element.getTime();
                    if (portSpeedMap.containsKey(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode())) {
                        String[] s = portSpeedMap.get(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode()).split("_");
                        double lastValue = Double.parseDouble(s[0]);
                        long lastTime = Long.parseLong(s[1]);
                        double currentValue = Double.parseDouble(element.getValue());
                        long currentTime = Long.parseLong(element.getTime());
                        double value = 0;
                        try {
                            /**
                             * 交换机返回的为累加统计，所以需要中间等待后，再次获取，计算两次的差
                             *  返回的值为byte，需要先乘8，计算为bit然后再除1024，计算进位量
                             */
                            value = 8 * Math.abs((lastValue - currentValue)) / 1024 / ((currentTime - lastTime) / 1000);
                        } catch (ArithmeticException exc) {
                            log.error("交换机端口出速率计算时，时间差为0.", exc);
                        }
                        out.collect(new DataStruct(element.getSystem_name(), element.getHost(), "102_101_103_104_105", element.getZbLastCode(), element.getNameCN(), element.getNameEN(), element.getTime(), String.valueOf(value)));
                        portSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), new_value);
                        portsOutSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), String.valueOf(value));
                        continue;
                    } else {
                        portSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), new_value);
                        continue;
                    }
                }

                //各端口入速率计算
                if (H3cSwitchCodeTypeEnum.IF_INOCTETS.getCode().equals(element.getZbFourName())) {
                    String new_value = element.getValue() + "_" + element.getTime();
                    if (portSpeedMap.containsKey(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode())) {
                        String[] s = portSpeedMap.get(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode()).split("_");
                        double lastValue = Double.parseDouble(s[0]);
                        long lastTime = Long.parseLong(s[1]);
                        double currentValue = Double.parseDouble(element.getValue());
                        long currentTime = Long.parseLong(element.getTime());
                        double value = 0;
                        try {
                            /**
                             * 交换机返回的为累加统计，所以需要中间等待后，再次获取，计算两次的差
                             *  返回的值为byte，需要先乘8，计算为bit然后再除1024，计算进位量
                             */
                            value = 8 * Math.abs((lastValue - currentValue)) / 1024 / ((currentTime - lastTime) / 1000);
                        } catch (ArithmeticException exc) {
                            log.error("交换机端口入速率计算时，时间差为0.", exc);
                        }
                        out.collect(new DataStruct(element.getSystem_name(), element.getHost(), "102_101_103_105_106", element.getZbLastCode(), element.getNameCN(), element.getNameEN(), element.getTime(), String.valueOf(value)));
                        portSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), new_value);
                        portsInSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), String.valueOf(value));
                        continue;
                    } else {
                        portSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(), new_value);
                        continue;
                    }
                }
            }
        }

        //板卡数量计算
        cardNumProcess(out);
        //端口数量计算
        portNumProcess(out);
        //所有端口出速率
        portsOutSpeedProcess(out);
        //所有端口入速率
        portsInSpeedProcess(out);
    }

    /**
     * 所有端口入速率计算
     *
     * @param out
     */
    private void portsInSpeedProcess(Collector<DataStruct> out) {
        if (isNotEmpty(portsInSpeedMap)) {
            double portsInSpeed = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "102_101_103_113_113";
            String zbLastCode = "";
            String nameCn = "端口入速率(所有端口总)";
            String nameEn = "ports_speed_in";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portsInSpeedMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_switch_2";
                double portInSpeed = Double.parseDouble(portsInSpeedMap.get(key1));
                portsInSpeed = portsInSpeed + portInSpeed;
            }
            String value = String.valueOf(portsInSpeed);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            portsInSpeedMap.clear();
        }
    }

    /**
     * 所有端口出速率计算
     *
     * @param out
     */
    private void portsOutSpeedProcess(Collector<DataStruct> out) {
        if (isNotEmpty(portsOutSpeedMap)) {
            double portsOutSpeed = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "102_101_103_112_112";
            String zbLastCode = "";
            String nameCn = "端口出速率(所有端口总)";
            String nameEn = "ports_speed_out";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portsOutSpeedMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_switch_2";
                double portOutSpeed = Double.parseDouble(portsInSpeedMap.get(key1));
                portsOutSpeed = portOutSpeed + portOutSpeed;
            }
            String value = String.valueOf(portsOutSpeed);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            portsOutSpeedMap.clear();
        }
    }

    /**
     * 端口数量计算
     *
     * @param out
     */
    private void portNumProcess(Collector<DataStruct> out) {
        if (isNotEmpty(portNumMap)) {
            double portNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "102_101_103_114_114";
            String zbLastCode = "";
            String nameCn = "端口数量";
            String nameEn = "port_num";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portNumMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_switch_2";
                portNum++;
            }
            String value = String.valueOf(portNum);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            portNumMap.clear();
        }
    }

    /**
     * 板卡数量计算
     *
     * @param out
     */
    private void cardNumProcess(Collector<DataStruct> out) {
        if (isNotEmpty(cardNumMap)) {
            double cardNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "102_101_104_101_101";
            String zbLastCode = "";
            String nameCn = "板卡数量";
            String nameEn = "card_num";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : cardNumMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_switch_2";
                cardNum++;
            }
            String value = String.valueOf(cardNum);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            cardNumMap.clear();
        }
    }

    private boolean isNotEmpty(Map<String, String> map) {
        return null != map && map.size() > 0;
    }
}
