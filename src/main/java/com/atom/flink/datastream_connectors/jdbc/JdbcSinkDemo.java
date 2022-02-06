package com.atom.flink.datastream_connectors.jdbc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * * 从指定socket读取数据，在flink中对单词进行统计计算，然后将计算结果实时写入到JDBC中
 * <p>
 * use flinkdb;
 * create table t_word_count(word varchar(255) primary key ,counts int)
 *
 * @author Atom
 */
public class JdbcSinkDemo {

    public static void main(String[] args) throws Exception {

        //创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpoint
        env.enableCheckpointing(5000);

        //创建DataStream
        //source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //调用Transformation开始
        //调用Transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        //调用Transformation结束


        //计算结果写入JDBC
        SinkFunction<Tuple2<String, Integer>> jdbcSink = JdbcSink.sink(
                "insert into t_word_count (word, counts) values (?, ?) on duplicate key update counts = ?",
                (statement, t) -> {
                    statement.setString(1, t.f0);
                    statement.setInt(2, t.f1);
                    statement.setInt(3, t.f1);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.0.200:3306/flinkdb?characterEncoding=utf8&useSSL=false")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("voo9ePh:")
                        .build()

        );

        summed.addSink(jdbcSink);

        env.execute();
    }
}
