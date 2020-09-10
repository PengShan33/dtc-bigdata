package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.enums.CodeTypeEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pengshan
 */
public class AixProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
    /**
     * 内存总大小
     */
    private Double memTotalSize;
    private Double memUsedSize;
    /**
     * kb转换为gb单位
     */
    private final Long kb2GbUnit = 1024 * 1024L;

    /**
     * bytes转换为gb单位
     */
    private final Long bytes2GbUnit = 1024 * 1024 * 1024L;
    /**
     * 每个分区簇的大小
     */
    private Map<String, String> cuSize = new HashMap<>(20);
    /**
     * 每个分区簇的数目
     */
    private Map<String, String> cuNum = new HashMap<>(20);
    /**
     * 每个分区已使用的簇的数目
     */
    private Map<String, String> cuUsedNum = new HashMap<>(20);
    /**
     * 每个盘的大小
     */
    private Map<String, String> diskBlockSize = new HashMap<>(20);
    /**
     * 每个盘剩余大小
     */
    private Map<String, String> diskFreeBlockSize = new HashMap<>(200);
    /**
     * 接收错误报文数
     */
    private Map<String, String> inErrorsMap = new HashMap<>(20);
    /**
     * 发送错误报文数
     */
    private Map<String, String> outErrorsMap = new HashMap<>(20);
    /**
     * 接收单播报文数
     */
    private Map<String, String> inUcastPktsMap = new HashMap<>(20);
    /**
     * 接收非单播报文数
     */
    private Map<String, String> inNUcastPktsMap = new HashMap<>(20);
    /**
     * 发送单播报文数
     */
    private Map<String, String> outUcastPktsMap = new HashMap<>(20);
    /**
     * 发送非单播报文数
     */
    private Map<String, String> outNUcastPktsMap = new HashMap<>(20);
    /**
     * 端口入流量
     */
    private Map<String, String> inOctetsMap = new HashMap<>(20);
    /**
     * 端口出流量
     */
    private Map<String, String> outOctetsMap = new HashMap<>(20);


    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> elements, Collector<DataStruct> out) throws Exception {

        for (DataStruct element : elements) {
            /**
             * 系统启动时间,暂时不做处理
             */
            if (CodeTypeEnum.AIX_SYSTEM_UP_TIME.getCode().equals(element.getZbFourName())) {
                continue;
            }

            /**
             * cpu使用率,直接使用
             */
            if (CodeTypeEnum.AIX_SECPU_UTILIZATION.getCode().equals(element.getZbFourName())) {
                String cpuUsedRate;
                String value = element.getValue();
                if (StringUtils.isEmpty(value)) {
                    cpuUsedRate = "0";
                } else {
                    cpuUsedRate = String.valueOf(Double.parseDouble(value) * 100);
                }
                out.collect(new DataStruct(element.getSystem_name(), element.getHost(), element.getZbFourName(), element.getZbLastCode(), element.getNameCN(), element.getNameEN(), element.getTime(), cpuUsedRate));
                continue;
            }

            /**
             * 内存总大小,KB转换为GB
             */
            if (CodeTypeEnum.AIX_MEMORY_SIZE.getCode().equals(element.getZbFourName())) {
                memTotalSize = Double.parseDouble(element.getValue()) / kb2GbUnit;
                String value = Double.parseDouble(element.getValue()) / kb2GbUnit + "";
                element.setValue(value);
                out.collect(element);
                continue;
            }

            /**
             * 每个分区簇的大小,单位Bytes
             */
            if (CodeTypeEnum.AIX_STORAGE_ALLOCATION_UNITS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuSize.put(key, value);
                continue;
            }

            /**
             * 每个分区簇的数目
             */
            if (CodeTypeEnum.AIX_STORAGE_SIZE.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuNum.put(key, value);
                continue;
            }

            /**
             * 每个分区已使用的簇的数目
             */

            if (CodeTypeEnum.AIX_STORAGE_USED.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuUsedNum.put(key, value);
                continue;
            }
            /**
             * 每个分区磁盘大小
             */
            if (CodeTypeEnum.AIX_FS_SIZE.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                diskBlockSize.put(key, value);
                continue;
            }

            /**
             * 每个分区磁盘剩余大小
             */
            if (CodeTypeEnum.AIX_FS_FREE.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                diskFreeBlockSize.put(key, value);
                continue;
            }
            /**
             * 接收错误报文数
             */
            if (CodeTypeEnum.AIX_IN_ERRORS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                inErrorsMap.put(key, value);
                continue;
            }

            /**
             * 发送错误报文数
             */
            if (CodeTypeEnum.AIX_OUT_ERRORS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                outErrorsMap.put(key, value);
                continue;
            }

            /**
             *接收到的单播报文数
             */
            if (CodeTypeEnum.AIX_IN_UCAST_PKTS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                inUcastPktsMap.put(key, value);
                continue;
            }

            /**
             * 接收到的非单播报文数
             */
            if (CodeTypeEnum.AIX_IN_NUCAST_PKTS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                inNUcastPktsMap.put(key, value);
                continue;
            }

            /**
             * 发送的单播报文数
             */
            if (CodeTypeEnum.AIX_OUT_UCAST_PKTS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                outUcastPktsMap.put(key, value);
                continue;
            }

            /**
             * 发送的非单播报文数
             */
            if (CodeTypeEnum.AIX_OUT_NUCAST_PKTS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                outNUcastPktsMap.put(key, value);
                continue;
            }

            /**
             * 端口入流量
             */
            if (CodeTypeEnum.AIX_IN_OCTETS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                inOctetsMap.put(key, value);
                continue;
            }
            /**
             * 端口出流量
             */
            if (CodeTypeEnum.AIX_OUT_OCTETS.getCode().equals(element.getZbFourName())) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                outOctetsMap.put(key, value);
                continue;
            }

        }
        // 已使用内存/内存使用率计算逻辑
        memoryUsageProcess(out);
        // 磁盘使用率计算逻辑
        diskUsageProcess(out);
        //发送错误报文数
        outErrorsProcess(out);
        //接收错误报文数
        inErrorsProcess(out);
        //发送报文数
        sendPackagesProcess(out);
        //接收报文数
        receivePackagesProcess(out);
        //端口入流量
        inOctetsProcess(out);
        //端口出流量
        outOctetsProcess(out);
    }

    /**
     * 端口出流量总计
     *
     * @param out
     */
    private void outOctetsProcess(Collector<DataStruct> out) {
        if (isNotEmpty(outOctetsMap)) {
            double outOctetsSize = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_118_118";
            String zbLastCode = "";
            String nameCn = "端口出流量总计";
            String nameEn = "aix_out_octets_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : outOctetsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                outOctetsSize += Double.parseDouble(outOctetsMap.get(key1));
            }
            String value = outOctetsSize / bytes2GbUnit + "";
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            outOctetsMap.clear();
        }
    }

    /**
     * 端口入流量总计
     *
     * @param out
     */
    private void inOctetsProcess(Collector<DataStruct> out) {
        if (isNotEmpty(inOctetsMap)) {
            double inOctetsSize = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_119_119";
            String zbLastCode = "";
            String nameCn = "端口入流量总计";
            String nameEn = "aix_in_octets_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : inOctetsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                inOctetsSize += Double.parseDouble(inOctetsMap.get(key1));
            }
            String value = inOctetsSize / bytes2GbUnit + "";
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            inOctetsMap.clear();
        }
    }

    /**
     * 接收报文数总计
     *
     * @param out
     */
    private void receivePackagesProcess(Collector<DataStruct> out) {
        if (isNotEmpty(inUcastPktsMap) && isNotEmpty(inNUcastPktsMap)) {
            long inUcastPktsNum = 0;
            long inNUcastPktsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_120_120";
            String zbLastCode = "";
            String nameCn = "接收报文数总计";
            String nameEn = "aix_net_packets_recv_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : inUcastPktsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                inUcastPktsNum += Long.parseLong(inUcastPktsMap.get(key1));
            }
            for (String key2 : inNUcastPktsMap.keySet()) {
                inNUcastPktsNum += Long.parseLong(inNUcastPktsMap.get(key2));
            }
            Long total = inUcastPktsNum + inNUcastPktsNum;
            String value = String.valueOf(total);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            inUcastPktsMap.clear();
            inNUcastPktsMap.clear();
        }
    }

    /**
     * 发送报文数总计
     *
     * @param out
     */
    private void sendPackagesProcess(Collector<DataStruct> out) {
        if (isNotEmpty(outUcastPktsMap) && isNotEmpty(outNUcastPktsMap)) {
            long outUcastPktsNum = 0;
            long outNUcastPktsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_121_121";
            String zbLastCode = "";
            String nameCn = "发送报文数总计";
            String nameEn = "aix_net_packets_sent_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : outUcastPktsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                outUcastPktsNum += Long.parseLong(outUcastPktsMap.get(key1));
            }
            for (String key2 : outNUcastPktsMap.keySet()) {
                outNUcastPktsNum += Long.parseLong(outNUcastPktsMap.get(key2));
            }
            Long total = outUcastPktsNum + outNUcastPktsNum;
            String value = String.valueOf(total);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            outUcastPktsMap.clear();
            outNUcastPktsMap.clear();
        }
    }

    /**
     * 接收错误报文数总计
     *
     * @param out
     */
    private void inErrorsProcess(Collector<DataStruct> out) {
        if (isNotEmpty(inErrorsMap)) {
            long inErrorsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_122_122";
            String zbLastCode = "";
            String nameCn = "接收错误报文数总计";
            String nameEn = "aix_net_err_in_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : inErrorsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                inErrorsNum += Long.parseLong(inErrorsMap.get(key1));
            }
            String value = String.valueOf(inErrorsNum);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            inErrorsMap.clear();
        }
    }

    /**
     * 发送错误报文数总计
     *
     * @param out
     */
    private void outErrorsProcess(Collector<DataStruct> out) {
        if (isNotEmpty(outErrorsMap)) {
            long outErrorsNum = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_105_123_123";
            String zbLastCode = "";
            String nameCn = "发送错误报文数总计";
            String nameEn = "aix_net_err_out_total";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : outErrorsMap.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                outErrorsNum += Long.parseLong(outErrorsMap.get(key1));
            }
            String value = String.valueOf(outErrorsNum);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            outErrorsMap.clear();
        }
    }

    /**
     * 磁盘使用率
     *
     * @param out
     */
    private void diskUsageProcess(Collector<DataStruct> out) {
        if (isNotEmpty(diskBlockSize) && isNotEmpty(diskFreeBlockSize)) {
            double diskSize = 0;
            double diskFreeSize = 0;
            double diskUsedRate;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_104_110_110";
            String zbLastCode = "";
            String nameCn = "磁盘使用率";
            String nameEn = "aix_disk_used_rate";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : diskBlockSize.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";

                diskSize += Double.parseDouble(diskBlockSize.get(key1));
            }
            for (String key2 : diskFreeBlockSize.keySet()) {
                diskFreeSize += Double.parseDouble(diskFreeBlockSize.get(key2));
            }
            diskUsedRate = 100 - diskFreeSize / diskSize * 100;
            String value = String.valueOf(diskUsedRate);
            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, nameCn, nameEn, time, value));
            diskBlockSize.clear();
        }
    }

    /**
     * 内存使用率
     *
     * @param out
     */
    private void memoryUsageProcess(Collector<DataStruct> out) {
        if (isNotEmpty(cuSize) && isNotEmpty(cuUsedNum)) {
            double usedSize = 0;
            String systemName = null;
            String host = null;
            String zbFourName = "101_102_103_103_103";
            String zbLastCode = "";
            String chineseName = "内存使用率";
            String englishName = "hr_memory_used_rate";
            String time = String.valueOf(System.currentTimeMillis());

            for (String key1 : cuSize.keySet()) {
                double size = Double.parseDouble(cuSize.get(key1));

                String[] split = key1.split("-");
                host = split[0];
                systemName = split[1].split("_")[0] + "_" + split[1].split("_")[1] + "|aix";
                String key2 = split[0] + "-" + "101_102_104_106_106" + "-" + split[2];
                double usedNum = Double.parseDouble(cuUsedNum.get(key2));
                usedSize = usedSize + size * usedNum / bytes2GbUnit;
            }

            double memUsedRate = usedSize / memTotalSize * 100;
            String value = String.valueOf(memUsedRate);

            out.collect(new DataStruct(systemName, host, zbFourName, zbLastCode, chineseName, englishName, time, value));
            cuSize.clear();
            cuUsedNum.clear();
        }
    }

    private boolean isNotEmpty(Map<String, String> map) {
        return null != map && map.size() > 0;
    }

}
