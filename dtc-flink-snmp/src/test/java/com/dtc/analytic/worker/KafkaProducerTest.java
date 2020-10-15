package com.dtc.analytic.worker;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author
 */
public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.3.7.232:9092,10.3.7.233:9092,10.3.6.20:9092");
        properties.setProperty("zookeeper.connect", "10.3.7.232:2181,10.3.7.233:2181,10.3.6.20:2181");
        DataStreamSource stream = env.addSource(new FileDataSource()).setParallelism(1);
        stream.print();
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("router-test", new SimpleStringSchema(), properties);
        stream.addSink(producer);
        env.execute("router data to kafka job.");
    }
}



