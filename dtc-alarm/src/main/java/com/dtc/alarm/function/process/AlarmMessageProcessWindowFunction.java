package com.dtc.alarm.function.process;

import com.dtc.alarm.domain.AlarmMessage;
import com.dtc.alarm.enums.AlarmLevelEnum;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple21;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author
 */
public class AlarmMessageProcessWindowFunction extends ProcessWindowFunction<AlarmMessage,
        Map<String, Tuple21<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String, Integer, String>>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<AlarmMessage> iterable,
                        Collector<Map<String, Tuple21<String, String, String, Double, Double, Double,
                                Double, String, String, String, String, String,
                                String, String, String, String, Integer, Integer, String, Integer, String>>> collector) throws Exception {

        Tuple21<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String,Integer, String> tuple21 = new Tuple21<>();

        Map<String, Tuple21<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String,Integer,String>> map = new HashMap<>();

        for (AlarmMessage alarmMessage : iterable) {
            Double number = alarmMessage.getNumber();
            String alarmLevel = alarmMessage.getAlarmLevel();
            tuple21.f0 = alarmMessage.getAssetId();
            tuple21.f1 = alarmMessage.getIpv4();
            tuple21.f2 = alarmMessage.getCode();
            String key = alarmMessage.getIpv4() + "." + alarmMessage.getCode().replace("_", ".");
            if (AlarmLevelEnum.GENERAL.getLevelCode().equals(alarmLevel)) {
                tuple21.f3 = number;
            } else if (AlarmLevelEnum.LESS_SERIOUS.getLevelCode().equals(alarmLevel)) {
                tuple21.f4 = number;
            } else if (AlarmLevelEnum.SERIOUS.getLevelCode().equals(alarmLevel)) {
                tuple21.f5 = number;
            } else if (AlarmLevelEnum.FATAL.getLevelCode().equals(alarmLevel)) {
                tuple21.f6 = number;
            }
            tuple21.f7 = alarmMessage.getAssetCode();
            tuple21.f8 = alarmMessage.getName();
            tuple21.f9 = alarmMessage.getConAssetId();
            tuple21.f10 = alarmMessage.getStrategyId();
            tuple21.f11 = alarmMessage.getTriggerKind();
            tuple21.f12 = alarmMessage.getTriggerName();
            tuple21.f13 = alarmMessage.getComparator();
            tuple21.f14 = alarmMessage.getUnit();
            tuple21.f15 = alarmMessage.getUpTime();
            tuple21.f16 = alarmMessage.getPastTimeSecond();
            tuple21.f17 = alarmMessage.getTarget();
            tuple21.f18 = alarmMessage.getConAlarm();
            tuple21.f19 = alarmMessage.getPastTime();
            tuple21.f20 = alarmMessage.getTimeUnit();

            map.put(key, tuple21);
        }
        collector.collect(map);
    }
}
