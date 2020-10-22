package com.dtc.alarm.function.process;

import com.dtc.alarm.domain.AlarmMessage;
import com.dtc.alarm.enums.AlarmLevelEnum;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @author
 */
public class AlarmMessageProcessWindowFunction extends ProcessWindowFunction<AlarmMessage,
        Map<String, Tuple19<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String>>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<AlarmMessage> iterable,
                        Collector<Map<String, Tuple19<String, String, String, Double, Double, Double,
                                Double, String, String, String, String, String,
                                String, String, String, String, Integer, Integer, String>>> collector) throws Exception {

        Tuple19<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String> tuple19 = new Tuple19<>();

        Map<String, Tuple19<String, String, String, Double, Double, Double,
                Double, String, String, String, String, String,
                String, String, String, String, Integer, Integer, String>> map = new HashMap<>();

        for (AlarmMessage alarmMessage : iterable) {
            Double number = alarmMessage.getNumber();
            String alarmLevel = alarmMessage.getAlarmLevel();
            tuple19.f0 = alarmMessage.getAssetId();
            tuple19.f1 = alarmMessage.getIpv4();
            tuple19.f2 = alarmMessage.getCode();
            String key = alarmMessage.getIpv4() + "." + alarmMessage.getCode().replace("_", ".");
            if (AlarmLevelEnum.GENERAL.getLevelCode().equals(alarmLevel)) {
                tuple19.f3 = number;
            } else if (AlarmLevelEnum.LESS_SERIOUS.getLevelCode().equals(alarmLevel)) {
                tuple19.f4 = number;
            } else if (AlarmLevelEnum.SERIOUS.getLevelCode().equals(alarmLevel)) {
                tuple19.f5 = number;
            } else if (AlarmLevelEnum.FATAL.getLevelCode().equals(alarmLevel)) {
                tuple19.f6 = number;
            }
            tuple19.f7 = alarmMessage.getAssetCode();
            tuple19.f8 = alarmMessage.getName();
            tuple19.f9 = alarmMessage.getConAssetId();
            tuple19.f10 = alarmMessage.getStrategyId();
            tuple19.f11 = alarmMessage.getTriggerKind();
            tuple19.f12 = alarmMessage.getTriggerName();
            tuple19.f13 = alarmMessage.getComparator();
            tuple19.f14 = alarmMessage.getUnit();
            tuple19.f15 = alarmMessage.getUpTime();
            tuple19.f16 = alarmMessage.getPastTimeSecond();
            tuple19.f17 = alarmMessage.getTarget();
            tuple19.f18 = alarmMessage.getConAlarm();

            map.put(key, tuple19);
        }
        collector.collect(map);
    }
}
