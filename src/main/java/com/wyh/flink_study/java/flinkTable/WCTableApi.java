package com.wyh.flink_study.java.flinkTable;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.ArrayList;

public class WCTableApi {
    public static void main(String[] args) {
        //创建flink批处理上下文环境
        ExecutionEnvironment fbEnv = ExecutionEnvironment.getExecutionEnvironment();
        //创建flink table的执行环境
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(fbEnv);

        //模拟数据
        String words = "hello flink hello shujia";
        //正常wordcount过程
        String[] split = words.split("\\W+");

        ArrayList<WC> list = new ArrayList<>();

        for (String word : split) {
            WC wc = new WC(word, 1L);
            list.add(wc);
        }

        //使用flink批处理环境封装成dataSet数据类型
        DataSet<WC> input = fbEnv.fromCollection(list);

        //使用flink table处理环境通过dataSet加载表
        Table table = tableEnvironment.fromDataSet(input, "word,Frequency");

        //打印表结构
        table.printSchema();

        //创建临时视图
        tableEnvironment.createTemporaryView("wordcount",table);

        Table table1 = tableEnvironment.sqlQuery("select word,sum(Frequency) as Frequency from wordcount group by word");

        //将表类型转换成dataSet类型
        DataSet<WC> ds3 = tableEnvironment.toDataSet(table1, WC.class);

        try {
            ds3.printToErr();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
