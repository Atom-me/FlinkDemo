package com.atom.flink.custom;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

/**
 * * run 方法，产生数据。有限数据流，数据读完就停掉了（run 方法执行完了）
 *
 * @author Atom
 */
public class CustomSource01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 ENV 并行度
        env.setParallelism(2);
        DataStreamSource<String> streamSource = env.addSource(new MySource1());

        System.err.println("这个自定义source的并行度=" + env.getParallelism());
        streamSource.print();

        env.execute();
    }


    // 自定义source
    public static class MySource1 implements SourceFunction<String> {

        /**
         * run 方法，产生数据。有限数据流，数据读完就停掉了（run 方法执行完了）
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            List<String> words = Arrays.asList("aaa", "bbb", "ccc", "ddd", "eee");
            for (String word : words) {
                sourceContext.collect(word);// 将 source 产生的数据输出
            }

        }

        @Override
        public void cancel() {


        }
    }
}
