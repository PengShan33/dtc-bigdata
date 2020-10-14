package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
import com.dtc.java.analytic.V2.map.function.*;
import com.dtc.java.analytic.V2.process.function.*;
import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
import com.dtc.java.analytic.V2.sink.opentsdb.PSinkToOpentsdb;
import com.dtc.java.analytic.V2.sink.redis.RedisWriter;
import com.dtc.java.analytic.V2.sink.redis.SinkToRedis;
import com.dtc.java.analytic.V2.source.mysql.ReadAlarmMessage;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtc.java.analytic.V2.alarm.AlarmUntils.getAlarm;
import static com.dtc.java.analytic.V2.alarm.PingAlarmUntils.getAlarmPing;
import static com.dtc.java.analytic.V2.worker.untils.MainUntils.*;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
public class StreamToFlinkV3 {

    /**
     * 布隆过滤器
     */
    static BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 1000000, 0.001);
    private static DataStream<Map<String, String>> alarmDataStream = null;

    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                "alarm_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);

        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        String opentsdb_url = parameterTool.get(PropertiesConstants.OPENTSDB_URL, "http://10.3.7.231:4399").trim();
        int windowSizeMillis = parameterTool.getInt(PropertiesConstants.WINDOWS_SIZE, 10 * 1000);
        TimesConstats build = getSize(parameterTool);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        asset_id, ipv4, strategy_kind, triger_name, number, code, alarm_level, asset_code, name
        DataStreamSource<Tuple9<String, String, String, String, Double, String, String, String, String>> alarmMessageMysql = env.addSource(new ReadAlarmMessage()).setParallelism(1);
        DataStream<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> process = alarmMessageMysql.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new MySqlProcessMapFunction());
        alarmDataStream = process.map(new MySQLFunction());
        BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);

        DataStreamSource<SourceEvent> streamSource = KafkaConfigUtil.buildSource(env);

//        {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());

        SplitStream<DataStruct> splitStream
                = getSplit(mapStream);
//        windows指标数据处理
        winDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, build);
//        linux指标数据处理
        linuxDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, build);
//        aix指标数据处理
        aixDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, build);
//        h3c交换机处理
        h3cDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, parameterTool, build);
//        中兴交换机
        zxDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, parameterTool, build);
//        dpi设备
        dpiDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, parameterTool, build);
//        路由器
        routerDataProcess(opentsdb_url, windowSizeMillis, broadcast, splitStream, parameterTool, build);
        env.execute("Dtc-Alarm-Flink-Process");
    }

    private static void routerDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool, TimesConstats build) {
        SingleOutputStreamOperator<DataStruct> routerProcess = splitStream
                .select("Router")
                .map(new RouterMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new RouterProcessMapFunction());

//        路由器数据全量写opentsdb
        routerProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
    }

    private static void winDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, TimesConstats build) {
        SingleOutputStreamOperator<DataStruct> winProcess = splitStream
                .select("Win")
                .map(new WinMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new WinProcessMapFunction());
//        机器网络是否联通
        DataStream<AlterStruct> alarmPing = getAlarmPing(winProcess, broadcast, build);
        alarmPing.addSink(new MysqlSink());
//        windows数据全量写opentsdb
        winProcess.addSink(new PSinkToOpentsdb(opentsdb_url));

//        windows数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmWindows = getAlarm(winProcess, broadcast, build);
        alarmWindows.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    private static void linuxDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, TimesConstats build) {
        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
                .select("Linux")
                .map(new LinuxMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new LinuxProcessMapFunction());

//        Linux数据全量写opentsdb
        linuxProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//        Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmLinux = getAlarm(linuxProcess, broadcast, build);
        alarmLinux.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    private static void aixDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, TimesConstats build) {
        SingleOutputStreamOperator<DataStruct> aixProcess = splitStream
                .select("Aix")
                .map(new AixMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new AixProcessMapFunction());

//        aix指标数据写入opentsdb
        aixProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//        aix数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmLinux = getAlarm(aixProcess, broadcast, build);
//        alarmLinux.forEach(e -> e.addSink(new MysqlSink()));
        alarmLinux.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    private static void h3cDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool, TimesConstats build) {
//        交换机指标数据处理
        SingleOutputStreamOperator<DataStruct> H3C_Switch = splitStream
                .select("H3C_Switch")
                .map(new H3cMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new H3CSwitchProcessMapFunction());

//        板卡等信息写入到redis中
        H3C_Switch.flatMap(new FlatMapFunction<DataStruct, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(DataStruct value, Collector<Tuple3<String, String, String>> out) throws Exception {
                String demo = value.getHost() + "_" + value.getZbFourName() + "_" + value.getZbLastCode();
                if (!bf.mightContain(demo)) {
                    if ("102_101_101_101_101".equals(value.getZbFourName()) || "102_101_103_107_108".equals(value.getZbFourName())) {
                        bf.put(demo);
                        out.collect(new Tuple3<>(value.getZbFourName(), value.getHost(), value.getZbLastCode()));
                    }
                }
            }
        }).addSink(new SinkToRedis());

//        Linux数据全量写opentsdb
        H3C_Switch.addSink(new PSinkToOpentsdb(opentsdb_url));
//        Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> H3C_Switch_1 = getAlarm(H3C_Switch, broadcast, build);
//        H3C_Switch_1.forEach(e -> e.addSink(new MysqlSink()));
        H3C_Switch_1.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    private static void zxDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool, TimesConstats build) {
//        交换机指标数据处理
        SingleOutputStreamOperator<DataStruct> ZX_Switch = splitStream
                .select("ZX_Switch")
                .map(new ZXMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new ZXSwitchProcessMapFunction());

//        板卡等信息写入到redis中
        ZX_Switch.flatMap(new FlatMapFunction<DataStruct, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(DataStruct value, Collector<Tuple3<String, String, String>> out) throws Exception {
                String demo = value.getHost() + "_" + value.getZbFourName() + "_" + value.getZbLastCode();
                if (!bf.mightContain(demo)) {
                    if ("102_103_101_101_101".equals(value.getZbFourName()) || "102_103_103_105_105".equals(value.getZbFourName())) {
                        bf.put(demo);
                        out.collect(new Tuple3<>(value.getZbFourName(), value.getHost(), value.getZbLastCode()));
                    }
                }
            }
        }).addSink(new SinkToRedis());

//        Linux数据全量写opentsdb
        ZX_Switch.addSink(new PSinkToOpentsdb(opentsdb_url));
//        Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> H3C_Switch_1 = getAlarm(ZX_Switch, broadcast, build);
//        H3C_Switch_1.forEach(e -> e.addSink(new MysqlSink()));
        H3C_Switch_1.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    private static void dpiDataProcess(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool, TimesConstats build) {
//        交换机指标数据处理
        SingleOutputStreamOperator<DataStruct> DPI_Switch = splitStream
                .select("DPI")
                .map(new DPIMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new DPISwitchProcessMapFunction());

        DPI_Switch.flatMap(new FlatMapFunction<DataStruct, Tuple3<String, String, String>>() {
            @Override
            public void flatMap(DataStruct value, Collector<Tuple3<String, String, String>> out) throws Exception {
                String demo = value.getHost() + "_" + value.getZbFourName() + "_" + value.getZbLastCode();
                if (!bf.mightContain(demo)) {
                    if ("103_102_101_101_101".equals(value.getZbFourName()) || "103_102_103_107_107_1".equals(value.getZbFourName())) {
                        bf.put(demo);
                        out.collect(new Tuple3<>(value.getZbFourName(), value.getHost(), value.getZbLastCode()));
                    }
                }
            }
        }).addSink(new SinkToRedis());
//        Linux数据全量写opentsdb
        DPI_Switch.addSink(new PSinkToOpentsdb(opentsdb_url));
//        Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> H3C_Switch_1 = getAlarm(DPI_Switch, broadcast, build);
//        H3C_Switch_1.forEach(e -> e.addSink(new MysqlSink()));
        H3C_Switch_1.forEach(alarmDataStream -> alarmDataStream.addSink(new RedisWriter()));
    }

    static class MySQLFunction implements MapFunction<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Map<String, String>> {
        //        (445,10.3.1.6,101_101_106_103,50.0,null,null,null)
        @Override
        public Map<String, String> map(Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> event) throws Exception {
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> entries : event.entrySet()) {
                Tuple9<String, String, String, Double, Double, Double, Double, String, String> value = entries.getValue();
                String key = entries.getKey();
                String asset_id = value.f0;
                String ip = value.f1;
                String code = value.f2;
                Double level_1 = value.f3;
                Double level_2 = value.f4;
                Double level_3 = value.f5;
                Double level_4 = value.f6;
                String asset_code = value.f7;
                String asset_name = value.f8;
                String str = asset_id + ":" + code + ":" + asset_code + ":" + asset_name + ":" + level_1 + "|" + level_2 + "|" + level_3 + "|" + level_4;
                map.put(key, str);
            }
            return map;
        }
    }

}
