package com.atom.flink.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

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
public class MapDemo2 {

    public static void main(String[] args) throws Exception {
//        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8);

        //自定义算子 MyMap ： 实现 mums.map(i -> i*2),
        SingleOutputStreamOperator<Integer> doubledNum = nums.transform("MyMap", TypeInformation.of(Integer.class), new StreamMap<>(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }));

        doubledNum.print();
        env.execute();


    }

}
