package com.dtc.alarm;

import com.dtc.alarm.domain.AlarmAssetIndexCode;
import com.dtc.alarm.domain.AlarmMessage;
import com.dtc.alarm.function.join.AlarmMessageSourceSelector;
import com.dtc.alarm.function.join.IndexCodeSourceSelector;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author
 */
public class AlarmDataStreamJoinHandler {

    public static DataStream<Tuple5<String, String, Integer, Integer, String>>
    join(DataStreamSource<AlarmMessage> alarmMessageSource, DataStreamSource<AlarmAssetIndexCode> indexCodeSource, int windowSizeMillis) {
        return alarmMessageSource.coGroup(indexCodeSource)
                .where(new AlarmMessageSourceSelector())
                .equalTo(new IndexCodeSourceSelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSizeMillis)))
                .apply(new CoGroupFunction<AlarmMessage, AlarmAssetIndexCode, Tuple5<String, String, Integer, Integer, String>>() {
                    @Override
                    public void coGroup(Iterable<AlarmMessage> leftNode,
                                        Iterable<AlarmAssetIndexCode> rightNode,
                                        Collector<Tuple5<String, String, Integer, Integer, String>> collector) throws Exception {
                        Tuple5<String, String, Integer, Integer, String> tuple5 = new Tuple5<>();

                        for (AlarmMessage node1 : leftNode) {
                            tuple5.f0 = node1.getCode();
                            tuple5.f1 = node1.getIpv4();
                            tuple5.f2 = node1.getTarget();
                            tuple5.f3 = node1.getPastTimeSecond();
                        }

                        for (AlarmAssetIndexCode node2 : rightNode) {
                            tuple5.f4 = node2.getSubCode();
                        }

                        collector.collect(tuple5);
                    }
                });

    }
}
