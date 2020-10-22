package com.dtc.alarm.function.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple19;

import java.util.HashMap;
import java.util.Map;

/**
 * @author
 */
public class AlarmMessageMapFunction implements MapFunction<Map<String, Tuple19<String, String, String, Double, Double,
        Double, Double, String, String, String, String,
        String, String, String, String, String, Integer, Integer, String>>, Map<String, String>> {


    @Override
    public Map<String, String> map(Map<String, Tuple19<String, String, String, Double, Double, Double,
            Double, String, String, String, String, String,
            String, String, String, String, Integer, Integer, String>> tuple19Map) throws Exception {

        Map<String, String> map = new HashMap<>(100);
        tuple19Map.forEach((k, v) -> {

            String assetId = v.f0;
            String ip = v.f1;
            String code = v.f2;
            Double levelOne = v.f3;
            Double levelTwo = v.f4;
            Double levelThree = v.f5;
            Double levelFour = v.f6;
            String assetCode = v.f7;
            String assetName = v.f8;
            String conAssetId = v.f9;
            String strategyId = v.f10;
            String triggerKind = v.f11;
            String triggerName = v.f12;
            String comparator = v.f13;
            String unit = v.f14;
            String upTime = v.f15;
            Integer pastTimeSecond = v.f16;
            Integer target = v.f17;
            String conAlarm = v.f18;
            StringBuffer buffer = new StringBuffer();
            buffer.append(assetId).append(":")
                    .append(code).append(":")
                    .append(assetCode).append(":")
                    .append(assetName).append(":")
                    .append(ip).append(":")
                    .append(conAssetId).append(":")
                    .append(strategyId).append(":")
                    .append(triggerKind).append(":")
                    .append(triggerName).append(":")
                    .append(comparator).append(":")
                    .append(unit).append(":")
                    .append(upTime).append(":")
                    .append(pastTimeSecond).append(":")
                    .append(target).append(":")
                    .append(conAlarm).append(":")
                    .append(levelOne).append("|")
                    .append(levelTwo).append("|")
                    .append(levelThree).append("|")
                    .append(levelFour).append("|");

            map.put(k, buffer.toString());
        });

        return map;
    }
}
