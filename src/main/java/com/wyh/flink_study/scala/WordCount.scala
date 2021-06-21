package com.wyh.flink_study.scala

import org.apache.flink.api.scala._

/**
  * 批处理代码
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的一个环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "D:\\IdeaProjects\\bigdataStu\\data\\wc.txt"

    val inputDataSet = env.readTextFile(inputPath)

    //分词之后做count
    val wordcountSet = inputDataSet
      .flatMap(lines => lines.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印
    wordcountSet.map(x => {
      x._1 + " " + x._2
    }).print()


  }

}