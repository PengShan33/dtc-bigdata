package com.dtc.alarm.util;

import com.dtc.alarm.constant.TimesConstats;
import com.dtc.alarm.domain.AlterStruct;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author
 */
public class AlarmUtil {

    public static List<DataStream<AlterStruct>> getAlarm(DataStream<Tuple6<String, String, Integer, Integer, String, Map<String, Object>>> event,
                                                         BroadcastStream<Map<String, String>> broadcast,
                                                         TimesConstats build) {

        SingleOutputStreamOperator<AlterStruct> alert_rule = event.filter(e -> !("107_107_101_101_101".equals(e.f0))).connect(broadcast)
                .process(getAlarmFunction());

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<AlterStruct, ?> alarmGrade =
                Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                        .where(new SimpleCondition<AlterStruct>() {
                            @Override
                            public boolean filter(AlterStruct s) {
                                return s.getLevel().equals("1");
                            }
                        }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("2");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("3");
                    }
                }).or(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct s) {
                        return s.getLevel().equals("4");
                    }
                }).times(build.getOne()).within(Time.seconds(build.getTwo()));
        Pattern<AlterStruct, ?> alarmIncream
                = Pattern.<AlterStruct>begin("begin", skipStrategy).subtype(AlterStruct.class)
                .where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("1");
                    }
                }).next("middle").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("2");
                    }
                }).next("three").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("3");
                    }
                }).next("finally").where(new SimpleCondition<AlterStruct>() {
                    @Override
                    public boolean filter(AlterStruct alterStruct) {
                        return alterStruct.getLevel().equals("4");
                    }
                }).times(build.getThree()).within(Time.seconds(build.getFour()));
        PatternStream<AlterStruct> patternStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmGrade);
        PatternStream<AlterStruct> alarmIncreamStream =
                CEP.pattern(alert_rule.keyBy(AlterStruct::getGaojing), alarmIncream);
        DataStream<AlterStruct> alarmStream =
                patternStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(0);
                    }
                });
        DataStream<AlterStruct> alarmStreamIncream =
                alarmIncreamStream.select(new PatternSelectFunction<AlterStruct, AlterStruct>() {
                    @Override
                    public AlterStruct select(Map<String, List<AlterStruct>> map) throws Exception {
                        return map.values().iterator().next().get(2);
                    }
                });
        List<DataStream<AlterStruct>> list = new ArrayList<>();
        list.add(alarmStream);
        list.add(alarmStreamIncream);
        return list;
    }

    private static BroadcastProcessFunction<Tuple6<String, String, Integer, Integer, String,
            Map<String, Object>>, Map<String, String>, AlterStruct> getAlarmFunction() {
        return new BroadcastProcessFunction<Tuple6<String, String, Integer, Integer, String,
                Map<String, Object>>, Map<String, String>, AlterStruct>() {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(Tuple6<String, String, Integer, Integer, String, Map<String, Object>> value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                //value(code,ipv4,target,pastTimeSecond,subcode,Map)
                //broadcast(Map(ip.code,string))
                //TODO 规则匹配
                double result = 0;
                Set<Map.Entry<String, Object>> entries = value.f5.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    result = Double.parseDouble(String.valueOf(entry.getValue()));
                }
                String code = value.f0.replace("_", ".");
                String ip = value.f1;
                String key = ip + "." + code;
                String rule = broadcastState.get(key).trim();
                String[] split = rule.split(":");
                String comparator = split[9];
                String levels = split[17];

                if (comparator.equals(">")) { RuleCompareUtil.bigger(result,levels,rule,out); }
                if (comparator.equals("<")) { RuleCompareUtil.lower(result,levels,rule,out); }
                if (comparator.equals("=")) { RuleCompareUtil.equal(result,levels,rule,out); }
                if (comparator.equals("!=")) { RuleCompareUtil.unequal(result,levels,rule,out); }
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> out) throws Exception {

                if (value == null || value.size() == 0) {
                    return;
                }

                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                Iterator<Map.Entry<String, String>> valueIterator = value.entrySet().iterator();
                while (valueIterator.hasNext()) {
                    Map.Entry<String, String> entry = valueIterator.next();
                    broadcastState.put(entry.getKey(), entry.getValue());
                }

                Iterator<Map.Entry<String, String>> broadcastIterator = broadcastState.entries().iterator();
                while (broadcastIterator.hasNext()) {
                    Map.Entry<String, String> entry = broadcastIterator.next();
                    if (!value.containsKey(entry.getKey())) {
                        broadcastIterator.remove();
                    }
                }
            }
        };
    }
}
