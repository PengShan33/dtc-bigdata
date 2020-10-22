package com.dtc.alarm;

import com.dtc.alarm.constant.PropertiesConstants;
import com.dtc.alarm.constant.TimesConstats;
import com.dtc.alarm.domain.AlarmAssetIndexCode;
import com.dtc.alarm.domain.AlarmMessage;
import com.dtc.alarm.domain.AlertStruct;
import com.dtc.alarm.function.map.AlarmMessageMapFunction;
import com.dtc.alarm.function.map.OpenTSDBFlatMapFunction;
import com.dtc.alarm.function.process.AlarmMessageProcessWindowFunction;
import com.dtc.alarm.sink.AlarmMessageWriter;
import com.dtc.alarm.source.mysql.AlarmAssetIndexCodeReader;
import com.dtc.alarm.source.mysql.AlarmMessageReader;
import com.dtc.alarm.util.AlarmUtil;
import com.dtc.alarm.util.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * @author
 */
@Slf4j
public class AlarmHandler {

    private static DataStream<Map<String, String>> alarmDataStream = null;

    public static void main(String[] args) throws Exception {
        {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);
            AlarmHandler handler = new AlarmHandler();
            ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
            int windowSizeMillis = parameterTool.getInt(PropertiesConstants.WINDOWS_SIZE, 10 * 1000);
            TimesConstats build = handler.getSize(parameterTool);
            StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
            env.getConfig().setGlobalJobParameters(parameterTool);

            DataStreamSource<AlarmMessage> alarmMessageSource = env.addSource(new AlarmMessageReader()).setParallelism(1);
            DataStreamSource<AlarmAssetIndexCode> indexCodeSource = env.addSource(new AlarmAssetIndexCodeReader()).setParallelism(1);

            DataStream<Map<String, Tuple19<String, String, String, Double, Double, Double,
                    Double, String, String, String, String, String,
                    String, String, String, String, Integer, Integer, String>>> dataStream = alarmMessageSource.keyBy("assetId", "code")
                    .timeWindow(Time.milliseconds(windowSizeMillis)).process(new AlarmMessageProcessWindowFunction());

            alarmDataStream = dataStream.map(new AlarmMessageMapFunction());
            BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);
            // code,ipv4,target,pastTimeSecond,subcode
            DataStream<Tuple5<String, String, Integer, Integer, String>> joinStream = AlarmDataStreamJoinHandler.join(alarmMessageSource, indexCodeSource, windowSizeMillis);

            //查询opentsdb
            DataStream<Tuple6<String, String, Integer, Integer, String, Map<String, Object>>> opentsdbSource = joinStream.flatMap(new OpenTSDBFlatMapFunction());

            //指标告警处理
            List<DataStream<AlertStruct>> alrmStream = AlarmUtil.getAlarm(opentsdbSource, broadcast, build);

            //写入mysql
            alrmStream.forEach(alarm -> alarm.addSink(new AlarmMessageWriter()));
            env.execute("dtc-alarm-job");
        }
    }

    private TimesConstats getSize(ParameterTool parameterTool) {
        int first = parameterTool.getInt("dtc.alarm.times.one", 1);
        int firstTime = parameterTool.getInt("dtc.alarm.time.long.one", 60000);
        int second = parameterTool.getInt("dtc.alarm.times.two", 1);
        int secondTime = parameterTool.getInt("dtc.alarm.time.long.two", 60000);
        TimesConstats build = TimesConstats.builder().one(first).two(firstTime).three(second).four(secondTime).build();
        return build;
    }
}
