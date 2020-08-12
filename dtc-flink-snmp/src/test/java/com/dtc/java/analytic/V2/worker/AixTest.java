package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.schemas.SourceEventSchema;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.map.function.AixMapFunction;
import com.dtc.java.analytic.V2.process.function.AixProcessMapFunction;
import com.dtc.java.analytic.V2.worker.untils.MainUntils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class AixTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.3.7.232:9092,10.3.7.233:9092,10.3.6.20:9092");
        props.put("group.id","test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        String topic = "DTCsnmpTest05";
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
//        DataStreamSource<String> aixSourceStream = env.addSource(consumer);
        FlinkKafkaConsumer<SourceEvent> consumer = new FlinkKafkaConsumer<>(topic, new SourceEventSchema(), props);
        consumer.setStartFromLatest();
        DataStreamSource<SourceEvent> aixSourceStream = env.addSource(consumer);

        aixSourceStream.print();

        SingleOutputStreamOperator<DataStruct> aixMapStream = aixSourceStream.map(new MainUntils.MyMapFunctionV3());
        aixMapStream.map(new AixMapFunction())
                    .keyBy("Host")
                    .timeWindow(Time.seconds(10))
                    .process(new AixProcessMapFunction());
//                    .print();

//        aixMapStream.print();

        env.execute("aix test");
    }
}
