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
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    //1、设置参数
    val params = ParameterTool.fromArgs(args)
    //2、设置运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //3、获取数据源
    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WindowWordCount example with default input data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }
    //4、是配置文件在 web 接口生效
    env.getConfig.setGlobalJobParameters(params)

    //5、设置 windowSize 和 slideSize
    val windowSize: Int = params.getInt("window", 250)
    val slideSize: Int = params.getInt("slide", 150)

    //6、transform
    val counts: DataStream[(String, Int)] = text.flatMap(_.toLowerCase().split("\","))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .countWindow(windowSize, slideSize)
      .sum(1)

    //7、发送消息
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    //8、执行程序
    env.execute()
  }
}
