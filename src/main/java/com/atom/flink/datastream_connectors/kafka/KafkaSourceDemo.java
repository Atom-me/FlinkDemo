package com.atom.flink.datastream_connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author Atom
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.200:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", "g1");
        properties.setProperty("enable.auto.commit", "true");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        lines.print();

        env.execute("hello kafka source");


    }
}
