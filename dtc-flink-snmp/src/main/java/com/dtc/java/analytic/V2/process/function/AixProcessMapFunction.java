package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AixProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
    // 内存总大小
    private Double memTotalSize;
    private Double memUsedSize;
    // 每个分区簇的大小
    private Map<String, String> cuSize = new HashMap();
    // 每个分区簇的数目
    private Map<String, String> cuNum = new HashMap();
    // 每个分区已使用的簇的数目
    private Map<String, String> cuUsedNum = new HashMap();

    // 每个盘的大小
    private Map<String, String> diskBlockSize = new HashMap();
    // 每个盘剩余大小
    private Map<String, String> diskFreeBlockSize = new HashMap();


    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> elements, Collector<DataStruct> out) throws Exception {
        for (DataStruct element : elements) {

            /**
             * 系统启动时间,暂时不做处理
             */
            if (element.getZbFourName().equals("101_102_101_108_108")) {
                continue;
            }

            /**
             * cpu使用率,直接使用
             */
            if (element.getZbFourName().equals("101_102_102_103_103")) {
                out.collect(element);
                continue;
            }

            /**
             * 内存总大小,KB转换为GB
             */
            if (element.getZbFourName().equals("101_102_103_101_101")) {
                memTotalSize = Double.parseDouble(element.getValue()) / 1048576;
                String value = Double.parseDouble(element.getValue()) / 1048576 + "";
                element.setValue(value);
                out.collect(element);
                continue;
            }

            /**
             * 每个分区簇的大小,单位Bytes
             */
            if (element.getZbFourName().equals("101_102_104_104_104")) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuSize.put(key, value);
                continue;
            }

            /**
             * 每个分区簇的数目
             */
            if (element.getZbFourName().equals("101_102_104_105_105")) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuNum.put(key, value);
                continue;
            }

            /**
             * 每个分区已使用的簇的数目
             */

            if (element.getZbFourName().equals("101_102_104_106_106")) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                cuUsedNum.put(key, value);
                continue;
            }
            /**
             * 每个分区磁盘大小
             */
            if (element.getZbFourName().equals("101_102_104_108_108")) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                diskBlockSize.put(key, value);
                continue;
            }

            /**
             * 每个分区磁盘剩余大小
             */
            if (element.getZbFourName().equals("101_102_104_109_109")) {
                String key = element.getHost() + "-" + element.getZbFourName() + "-" + element.getZbLastCode();
                String value = element.getValue();
                diskFreeBlockSize.put(key, value);
                continue;
            }
        }

        // 已使用内存/内存使用率计算逻辑
        if (cuSize.size() != 0 && cuUsedNum.size() != 0) {
            double usedSize = 0;
            String System_name = null;
            String host = null;
            String zbFourName = "101_102_103_103_103";
            String zbLastCode = "";
            String nameCN = "内存使用率";
            String nameEN = "hr_memory_used_rate";
            String time = String.valueOf(System.currentTimeMillis());

            for (String key1 : cuSize.keySet()) {
                double size = Double.parseDouble(cuSize.get(key1));

                String[] split = key1.split("-");
                host = split[0];
                System_name = split[1].split("_")[0] + "_" + split[1].split("_")[1];
                String key2 = split[0] + "-" + "101_102_104_106_106" + "-" + split[2];
                double usedNum = Double.parseDouble(cuUsedNum.get(key2));
                usedSize = usedSize + size * usedNum / 1073741824;
            }

            double memUsedRate = usedSize / memTotalSize;
            String value = String.valueOf(memUsedRate);

            out.collect(new DataStruct(System_name, host, zbFourName, zbLastCode, nameCN, nameEN, time, value));
        }

        // 磁盘使用率计算逻辑
        if (diskBlockSize.size() != 0 && diskFreeBlockSize.size() != 0) {
            double diskSize = 0;
            double diskFreeSize = 0;
            double diskUsedRate = 0;
            String System_name = null;
            String host = null;
            String zbFourName = "101_102_104_110_110";
            String zbLastCode = "";
            String nameCN = "磁盘使用率";
            String nameEN = "aix_disk_used_rate";
            String time = String.valueOf(System.currentTimeMillis());
            for (String key1 : diskBlockSize.keySet()) {
                String[] split = key1.split("-");
                host = split[0];
                System_name = split[1].split("_")[0] + "_" + split[1].split("_")[1];

                diskSize += Double.parseDouble(diskBlockSize.get(key1));
            }
            for (String key2 : diskFreeBlockSize.keySet()) {
                diskFreeSize += Double.parseDouble(diskFreeBlockSize.get(key2));
            }
            diskUsedRate = 1 - diskFreeSize / diskSize;
            String value = String.valueOf(diskUsedRate);
            out.collect(new DataStruct(System_name,host,zbFourName,zbLastCode,nameCN,nameEN,time,value));
        }
    }

}
