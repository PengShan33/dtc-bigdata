package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.enums.H3cSwitchCodeTypeEnum;
import com.dtc.java.analytic.V2.enums.ZxSwitchCodeTypeEnum;
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
 * 华三交换机处理逻辑
 *
 */
@Slf4j
public class ZxSwitchProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
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
                if (ZxSwitchCodeTypeEnum.CPU_PERCET.getCode().equals(element.getZbFourName())) {
                    String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                    String value = element.getValue();
                    cardNumMap.put(key, value);
                    //板卡编码写入后端mysql库
                    MySqlSinkUtils.subCodeSinktoMysql(element,parameters);

                    out.collect(element);
                    continue;
                }

                //内存使用率,直接使用
                if (ZxSwitchCodeTypeEnum.MEM_USED_PERCET.getCode().equals(element.getZbFourName())) {
                    out.collect(element);
                    continue;
                }

                //端口工作状态,直接使用
                if (ZxSwitchCodeTypeEnum.PORT_STATUS.getCode().equals(element.getZbFourName())) {
                    String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                    String value = element.getValue();
                    portNumMap.put(key, value);
                    //端口编码写入后端mysql库
                    MySqlSinkUtils.subCodeSinktoMysql(element,parameters);

                    out.collect(element);
                    continue;
                }

                //各端口出速率,直接使用
                if (ZxSwitchCodeTypeEnum.PORT_SPEED_OUT.getCode().equals(element.getZbFourName())) {
                    out.collect(element);
                    portsOutSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(),element.getValue());
                    continue;
                }

                //各端口入速率,直接使用
                if (ZxSwitchCodeTypeEnum.PORT_SPEED_IN.getCode().equals(element.getZbFourName())) {
                    out.collect(element);
                    portsInSpeedMap.put(element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode(),element.getValue());
                    continue;
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
            String zbFourName = "102_103_103_113_113";
            String zbLastCode = "";
            String nameCn = "端口入速率(所有端口总)";
            String nameEn = "ports_speed_in";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portsInSpeedMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|zx_switch_2";
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
            String zbFourName = "102_103_103_112_112";
            String zbLastCode = "";
            String nameCn = "端口出速率(所有端口总)";
            String nameEn = "ports_speed_out";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portsOutSpeedMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|zx_switch_2";
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
            String zbFourName = "102_103_103_114_114";
            String zbLastCode = "";
            String nameCn = "端口数量";
            String nameEn = "port_num";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : portNumMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|zx_switch_2";
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
            String zbFourName = "102_103_104_103_103";
            String zbLastCode = "";
            String nameCn = "板卡数量";
            String nameEn = "card_num";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : cardNumMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|zx_switch_2";
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
