package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.enums.RouterCodeTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 路由器数据处理
 *
 * @author
 */
@Slf4j
public class RouterProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {

    /**
     * byte转换为gb单位
     */
    private final Long bytes2GbUnit = 1024 * 1024 * 1024L;

    /**
     * 获取端口入方向错包数
     */
    private Map<String, String> inErrorsMap = new HashMap<>(100);

    /**
     * 获取端口出方向错包数
     */
    private Map<String, String> outErrorsMap = new HashMap<>(100);

    /**
     * 获取端口入方向字节数
     */
    private Map<String, String> inOctetsMap = new HashMap<>(100);

    /**
     * 获取端口出方向字节数
     */
    private Map<String, String> outOctetsMap = new HashMap<>(100);

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> collector) throws Exception {

        for (DataStruct in : iterable) {
            String code = in.getZbFourName();
            //判断是否是数据
            boolean strResult = in.getValue().matches("(^-?[0-9][0-9]*(.[0-9]+)?)$");
            if (!strResult) {
                log.info("Value is not number of string!");
            } else {
                if (RouterCodeTypeEnum.ROUTER_CPU_USAGE.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_MEM_USAGE.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_OPER_STATUS.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_SLOT_CPURATIO.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_SLOT_MEMRATIO.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_SYS_CPURATIO.getCode().equals(code) ||
                        RouterCodeTypeEnum.ROUTER_SYS_MEMRATIO.getCode().equals(code)) {

                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;

                }

                if (RouterCodeTypeEnum.ROUTER_IN_ERRORS.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    inErrorsMap.put(key, value);
                    continue;
                }

                if (RouterCodeTypeEnum.ROUTER_OUT_ERRORS.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    outErrorsMap.put(key, value);
                    continue;
                }

                if (RouterCodeTypeEnum.ROUTER_HCIN_OCTETS.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    inOctetsMap.put(key, value);
                    continue;
                }

                if (RouterCodeTypeEnum.ROUTER_HCOUT_OCTETS.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    outOctetsMap.put(key, value);
                    continue;
                }
            }
        }
        // 获取端口入方向错包数
        inErrorsProcess(collector);
        // 获取端口出方向错包数
        outErrorsProcess(collector);
        // 获取端口入方向字节数
        inOctetsProcess(collector);
        // 获取端口出方向字节数
        outOctetsProcess(collector);
    }


    /**
     * 获取端口入方向错包数处理
     *
     * @param collector
     */
    private void inErrorsProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(inErrorsMap)) {
            long inErrorsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "103_101_101_106_106";
            String zbLastCode = "";
            String nameCn = "端口入方向错误包总计";
            String nameEn = "router_in_errors_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key : inErrorsMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|router";
                inErrorsNum += Long.parseLong(inErrorsMap.get(key));
            }
            String value = String.valueOf(inErrorsNum);
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            inErrorsMap.clear();
        }
    }

    /**
     * 获取端口出方向错包数处理
     *
     * @param collector
     */
    private void outErrorsProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(outErrorsMap)) {
            long outErrorsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "103_101_101_107_107";
            String zbLastCode = "";
            String nameCn = "端口出方向错误包总计";
            String nameEn = "router_out_errors_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key : outErrorsMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|router";
                outErrorsNum += Long.parseLong(outErrorsMap.get(key));
            }
            String value = String.valueOf(outErrorsNum);
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            outErrorsMap.clear();
        }
    }

    /**
     * 获取端口入方向字节数处理
     *
     * @param collector
     */
    private void inOctetsProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(inOctetsMap)) {
            double inOctetsSize = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "103_101_101_108_108";
            String zbLastCode = "";
            String nameCn = "端口入方向流量统计";
            String nameEn = "router_in_octets_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key : inOctetsMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|router";

                inOctetsSize += Double.parseDouble(inOctetsMap.get(key));
            }
            String value = inOctetsSize / bytes2GbUnit + "";
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            inOctetsMap.clear();
        }
    }

    /**
     * 获取端口出方向字节数处理
     *
     * @param collector
     */
    private void outOctetsProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(outOctetsMap)) {
            double outOctetsSize = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "103_101_101_109_109";
            String zbLastCode = "";
            String nameCn = "端口出方向流量统计";
            String nameEn = "router_out_octets_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key : outOctetsMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|router";

                outOctetsSize += Double.parseDouble(outOctetsMap.get(key));
            }
            String value = outOctetsSize / bytes2GbUnit + "";
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            outOctetsMap.clear();
        }
    }

    private boolean isNotEmpty(Map<String, String> map) {
        return null != map && map.size() > 0;
    }
}
