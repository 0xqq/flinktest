package flink.streaming.practice

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /10/11
  * @Project:flinktest
  * @Package:flink.streaming
  */
object SocketWindowWordCount {

  case class WordWithCount(word:String, count:Long)
  def main(args: Array[String]): Unit = {
    var hostname: String = "localhost"
    var port: Int = 0
    try {
      val params: ParameterTool = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
      port = params.getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }
    //获取运行版本
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //获取数据源
    val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')
    //分组、窗口操作
    val windowCounts: DataStream[WordWithCount] = text.flatMap(_.split("\\s")).filter(_.nonEmpty).map(w=> WordWithCount(w,1)).keyBy("word").timeWindow(Time.seconds(5)).sum("count")
    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    //启动 job
    env.execute("Socket Window WordCount")
  }
}
