package com.atom.flink.datastream_connectors.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * 出现Connection refused ，是端口没有被监听，StreamExecutionEnvironment 的socketTextStream 方法并不是自己监听一个端口，起一个服务队，等你给他发消息。
 * 是你先得通过nc 启动这个端口，然后flink再与之建立socket连接。
 * <p>
 * nc -l -p 8888
 *
 * @author Atom
 */
public class KafkaSinkDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        int parallelism = env.getParallelism();
        System.out.println("执行环境默认的并行度=" + parallelism);

        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 8888);
        int parallelism1 = lines.getParallelism();
        System.out.println("socket source 的并行度=" + parallelism1);


        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>("192.168.0.200:9092", "test1", new SimpleStringSchema());
        lines.addSink(flinkKafkaProducer);

        env.execute();
    }
}
