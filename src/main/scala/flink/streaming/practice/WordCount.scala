package flink.streaming.practice

import flink.util.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /10/11
  * @Project:flinktest
  * @Package:flink.streaming
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1、checking input parameters
    val param: ParameterTool = ParameterTool.fromArgs(args)

    //2、setup the execution envrionment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //3、make parameter availiable in web interface
    env.getConfig.setGlobalJobParameters(param)

    //4、get input data
    //4.1、read textfile from the given path
    val text =
    if (param.has("input")) {
      env.readTextFile(param.get("input"))
    } else {
      println("Executing WordCount example with default inputs data set.")
      println("Use --input to specify file input.")
      // get default test text data
      // X: _* 是一个整体操作符，将X转变为参数序列
      env.fromElements(WordCountData.WORDS: _*)
    }

    //5、transform
    // split up the lines in pairs (2-tuples) containing: (word,1)
    // \\w+为正则匹配，为匹配前一个字符
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty).map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0).sum(1)

    // emit result
    if (param.has("output")) {
      counts.writeAsText(param.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }
}
