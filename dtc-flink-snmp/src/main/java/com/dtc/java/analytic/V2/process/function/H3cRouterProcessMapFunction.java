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
public class H3cRouterProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {


    /**
     * 获取所有端口数量
     */
    private Map<String, String> operNumMap = new HashMap<>(100);

    /**
     * 获取所有单板数量
     */
    private Map<String, String> slotNumMap = new HashMap<>(100);

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> collector) throws Exception {

        for (DataStruct in : iterable) {
            String code = in.getZbFourName();
            //判断是否是数据
            boolean strResult = in.getValue().matches("(^-?[0-9][0-9]*(.[0-9]+)?)$");
            if (!strResult) {
                log.info("Value is not number of string!");
            } else {
                if (RouterCodeTypeEnum.ROUTER_SLOT_MEMRATIO.getCode().equals(code)) {
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;

                }

                if (RouterCodeTypeEnum.ROUTER_OPER_STATUS.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    operNumMap.put(key, value);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
                }

                if (RouterCodeTypeEnum.ROUTER_SLOT_CPURATIO.getCode().equals(code)) {
                    String key = in.getHost() + "-" + in.getZbFourName() + "-" + in.getZbLastCode();
                    String value = in.getValue();
                    slotNumMap.put(key, value);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
                }

            }
        }
        // 获取所有端口数量
        operNumProcess(collector);
        // 获取所有单板数量
        slotNumProcess(collector);
    }


    /**
     * 获取所有端口数量
     *
     * @param collector
     */
    private void operNumProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(operNumMap)) {
            long operNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "104_101_101_110_110";
            String zbLastCode = "";
            String nameCn = "所有端口数量";
            String nameEn = "router_oper_num";
            String time = String.valueOf(System.currentTimeMillis());

            for (String key : operNumMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_router";
                operNum++;
            }

            String value = String.valueOf(operNum);
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            operNumMap.clear();
        }
    }

    /**
     * 获取所有单板数量
     *
     * @param collector
     */
    private void slotNumProcess(Collector<DataStruct> collector) {
        if (isNotEmpty(slotNumMap)) {
            long slotNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "104_101_102_104_104";
            String zbLastCode = "";
            String nameCn = "所有单板数量";
            String nameEn = "router_slot_num";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key : slotNumMap.keySet()) {
                String[] split = key.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|h3c_router";
                slotNum++;
            }
            String value = String.valueOf(slotNum);
            collector.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            slotNumMap.clear();
        }
    }

    private boolean isNotEmpty(Map<String, String> map) {
        return null != map && map.size() > 0;
    }
}
