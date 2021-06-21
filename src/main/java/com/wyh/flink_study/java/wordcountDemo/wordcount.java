package com.wyh.flink_study.java.wordcountDemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class wordcount {
    public static void main(String[] args) {
        //创建flink执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "D:\\IdeaProjects\\bigdataStu\\data\\wc.txt";
        //从文件中读取数据
        DataSet<String> textFile = env.readTextFile(inputPath);

        //对数据集进行处理
        AggregateOperator<Tuple2<String, Integer>> result = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = s.split(" ");

                for (String word : words) {
                    if (word.length() > 0) {
                        out.collect(new Tuple2<String, Integer>(word, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);

        try {
            result.printToErr();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
