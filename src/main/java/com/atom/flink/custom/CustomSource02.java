package com.atom.flink.custom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * * run 方法，产生数据。无限数据流
 *
 * @author Atom
 */
public class CustomSource02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 ENV 并行度
        env.setParallelism(2);
        DataStreamSource<String> streamSource = env.addSource(new MySource2());

        System.err.println("这个自定义source的并行度=" + env.getParallelism());
        streamSource.print();

        env.execute();
    }


    /**
     * 自定义source
     * 也可以继承 RichParallelSourceFunction
     */
    public static class MySource2 extends RichParallelSourceFunction<String> {

        //1. 调用MySource2构造方法
        //2. 调用open方法，调用一次
        //3. 调用run方法
        //4. 调用cancel方法停止
        //5. 调用close方法释放资源


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("invoke open method....");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("invoke close method ...");
        }

        /**
         * run 方法，产生数据。有限数据流，数据读完就停掉了（run 方法执行完了）
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            int indexOfThisSubTask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("subTask: " + indexOfThisSubTask + " invoke run method ...");
            while (true) {
                sourceContext.collect(UUID.randomUUID().toString());
                TimeUnit.MILLISECONDS.sleep(2000);
            }

        }

        @Override
        public void cancel() {
            System.out.println("invoke cancel method...");


        }
    }
}
