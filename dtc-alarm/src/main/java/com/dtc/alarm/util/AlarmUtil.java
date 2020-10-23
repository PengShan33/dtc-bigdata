package com.dtc.alarm.util;

import com.dtc.alarm.constant.TimesConstats;
import com.dtc.alarm.domain.AlertStruct;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author
 */
public class AlarmUtil {

    public static List<DataStream<AlertStruct>> getAlarm(DataStream<Tuple6<String, String, Integer, Integer, String, Map<String, Object>>> event,
                                                         BroadcastStream<Map<String, String>> broadcast,
                                                         TimesConstats build) {

        SingleOutputStreamOperator<AlertStruct> alert_rule = event.filter(e -> !("107_107_101_101_101".equals(e.f0))).connect(broadcast)
                .process(getAlarmFunction());

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<AlertStruct, ?> alarmGrade =
                Pattern.<AlertStruct>begin("begin", skipStrategy).subtype(AlertStruct.class)
                        .where(new SimpleCondition<AlertStruct>() {
                            @Override
                            public boolean filter(AlertStruct s) {
                                return s.getLevel().equals("1");
                            }
                        }).or(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct s) {
                        return s.getLevel().equals("2");
                    }
                }).or(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct s) {
                        return s.getLevel().equals("3");
                    }
                }).or(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct s) {
                        return s.getLevel().equals("4");
                    }
                }).times(build.getOne()).within(Time.seconds(build.getTwo()));
        Pattern<AlertStruct, ?> alarmIncream
                = Pattern.<AlertStruct>begin("begin", skipStrategy).subtype(AlertStruct.class)
                .where(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct alterStruct) {
                        return alterStruct.getLevel().equals("1");
                    }
                }).next("middle").where(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct alterStruct) {
                        return alterStruct.getLevel().equals("2");
                    }
                }).next("three").where(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct alterStruct) {
                        return alterStruct.getLevel().equals("3");
                    }
                }).next("finally").where(new SimpleCondition<AlertStruct>() {
                    @Override
                    public boolean filter(AlertStruct alterStruct) {
                        return alterStruct.getLevel().equals("4");
                    }
                }).times(build.getThree()).within(Time.seconds(build.getFour()));
        PatternStream<AlertStruct> patternStream =
                CEP.pattern(alert_rule.keyBy(AlertStruct::getGaojing), alarmGrade);
        PatternStream<AlertStruct> alarmIncreamStream =
                CEP.pattern(alert_rule.keyBy(AlertStruct::getGaojing), alarmIncream);
        DataStream<AlertStruct> alarmStream =
                patternStream.select(new PatternSelectFunction<AlertStruct, AlertStruct>() {
                    @Override
                    public AlertStruct select(Map<String, List<AlertStruct>> map) throws Exception {
                        return map.values().iterator().next().get(0);
                    }
                });
        DataStream<AlertStruct> alarmStreamIncream =
                alarmIncreamStream.select(new PatternSelectFunction<AlertStruct, AlertStruct>() {
                    @Override
                    public AlertStruct select(Map<String, List<AlertStruct>> map) throws Exception {
                        return map.values().iterator().next().get(2);
                    }
                });
        List<DataStream<AlertStruct>> list = new ArrayList<>();
        list.add(alarmStream);
        list.add(alarmStreamIncream);
        return list;
    }

    private static BroadcastProcessFunction<Tuple6<String, String, Integer, Integer, String,
            Map<String, Object>>, Map<String, String>, AlertStruct> getAlarmFunction() {
        return new BroadcastProcessFunction<Tuple6<String, String, Integer, Integer, String,
                Map<String, Object>>, Map<String, String>, AlertStruct>() {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(Tuple6<String, String, Integer, Integer, String, Map<String, Object>> value, ReadOnlyContext ctx, Collector<AlertStruct> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);

                //TODO 规则匹配
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlertStruct> out) throws Exception {

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
