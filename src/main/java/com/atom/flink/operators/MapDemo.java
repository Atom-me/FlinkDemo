package com.atom.flink.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Locale;

/**
 * Map 算子
 * <p>
 * ➜  ~ nc -l -p 8888
 * java
 * spark
 * flink
 * xxx yyy
 *
 * @author Atom
 */
public class MapDemo {

    public static void main(String[] args) throws Exception {
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //输入一行一个单词
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        //将输入单词变大写
        SingleOutputStreamOperator<String> upperWord = words.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase(Locale.ROOT);
            }
        });

        upperWord.print();

        env.execute();


    }

}
