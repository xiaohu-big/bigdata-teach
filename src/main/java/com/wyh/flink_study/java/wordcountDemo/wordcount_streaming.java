package com.wyh.flink_study.java.wordcountDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class wordcount_streaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> demo = streamEnv.socketTextStream("demo", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = demo.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> tuple2TupleTimeWindowWindowedStream = tuple2TupleKeyedStream.timeWindow(Time.seconds(5), Time.seconds(1));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2TupleTimeWindowWindowedStream.sum(1);

        sum.print().setParallelism(1);
        streamEnv.execute("streaming_wordcount");
    }
}
