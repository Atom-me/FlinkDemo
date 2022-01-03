package com.atom.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 1。 创建 topic
 * /opt/kafka/bin/kaftopics.sh --create --bootstrap-server localhost:9092 --topic cdn-log
 * <p>
 *
 *
 * <p>
 * 2 。启动作业，向topic里面发送一些数据，观察控制台打印的消息
 *
 * 3。 kafka 发送消息
 * bash-5.1# /opt/kafka/bin/kafka-console-producer.sh --topic=cdn-log --broker-list localhost:9092
 * {"msg":"welcome flink users ..."}
 *
 * @author Atom
 */
public class Kafka2Print {
    public static void main(String[] args) throws Exception {
        String sourceDDL = "create table kafka_source (\n" +
                "msg string\n" +
                ") with (\n" +
                "  'connector'='kafka-0.11',\n" +
                "  'topic'='cdn-log',\n" +
                "  'properties.bootstrap.servers'='192.168.0.200:9092',\n" +
                "  'format'='json',\n" +
                "  'scan.startup.mode'='latest-offset'\n" +
                ")";


        // print sink
        String sinkDDL = "create table print_sink(\n" +
                "  msg string\n" +
                ") with(\n" +
                "  'connector'='print'\n" +
                ")";




        //创建执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        //数据提取
        Table sourceTab = tEnv.from("kafka_source");
        sourceTab.insertInto("print_sink");

        // 执行作业
        tEnv.execute("Flink hello world");


    }
}
