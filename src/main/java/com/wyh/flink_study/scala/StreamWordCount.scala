package com.wyh.flink_study.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建一个流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //为了host和port不写死，flink提供了一个方法
    val params = ParameterTool.fromArgs(args)

    //    val host = params.get("host")
    //
    //    val port = params.getInt("port")

    //env.disableOperatorChaining()//全局打散  一个算子一个任务
    //每一个算子也会有个方法  .disableChaining() 将这个算子单独拿出来
    //还有个方法.startNewChain() 将当前算子之前面和后面 分开

    //部署到集群中接收socket数据流
    //    val dataStream: DataStream[String] = env.socketTextStream(host, port)

    //接收socket数据流
    val dataStream = env.socketTextStream("localhost", 9999)

    //逐一读取数据，打散进行WordCount
    val wordCountStream = dataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountStream.print().setParallelism(1)


    //比批处理多一个步骤
    //真正执行这个任务，启动它的Executor
    env.execute("WordCountStream")


  }

}