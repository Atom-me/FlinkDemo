package com.atom.flink.sql_connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 执行运行程序，打印随机数据即可，体验flink提供的内置随机source和打印sink
 *
 * @author Atom
 */
public class App {
    public static void main(String[] args) throws Exception {


        String sourceDDL = "create table random_source (\n" +
                "  f_sequence int,\n" +
                "  f_random int,\n" +
                "  f_random_str string\n" +
                ") with (\n" +
                "  'connector'='datagen',\n" +
                "  'row-per-second'='5',\n" +
                "  'fields.f_sequence.kind'='sequence',\n" +
                "  'fields.f_sequence.start'='1',\n" +
                "  'fields.f_sequence.end'='1000',\n" +
                "  'fields.f_random.min'='1',\n" +
                "  'fields.f_random.max'='1000',\n" +
                "  'fields.f_random_str.length'='10'\n" +
                ")";

        //为了方便测试，flink 提供了控制台打印的print。
        String sinkDDL = "create table print_sink (\n" +
                "  f_sequence int,\n" +
                "  f_random int,\n" +
                "  f_random_str string\n" +
                ") with (\n" +
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
        Table sourceTab = tEnv.from("random_source");
        sourceTab.insertInto("print_sink");

        // 执行作业
        tEnv.execute("Flink hello world");


    }
}
