package com.atom.flink.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 0。 创建 topic
 * /opt/kafka/bin/kaftopics.sh --create --bootstrap-server localhost:9092 --topic cdn-log
 * <p>
 * 1。 创建 mysql 表
 * create table cdn_log(msg varchar(255));
 * 2 。启动作业，向topic里面发送一些数据，观察控制台打印的消息
 * <p>
 * 3。 kafka 发送消息
 * bash-5.1# /opt/kafka/bin/kafka-console-producer.sh --topic=cdn-log --broker-list localhost:9092
 * {"msg":"welcome flink users ..."}
 *
 * @author Atom
 */
public class Kafka2MySQL {

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
        String sinkDDL = "create table mysql_sink(\n" +
                "  msg string\n" +
                ") with(\n" +
                "  'connector'='jdbc',\n" +
                "  'url'='jdbc:mysql://192.168.0.200:3306/flinkdb?characterEncoding=utf8&useSSL=false',\n" +
                "  'table-name'='cdn_log',\n" +
                "  'username'='root',\n" +
                "  'password'='voo9ePh:',\n" +
                "  'sink.buffer-flush.max-rows'='1',\n" +
                "  'sink.buffer-flush.interval'='1s'\n" +
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
        sourceTab.insertInto("mysql_sink");

        // 执行作业
        tEnv.execute("Flink hello world");
    }
}
