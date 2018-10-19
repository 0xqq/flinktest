package flink.streaming.practice.kafka

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /10/16
  * @Project:flinktest
  * @Package:flink.streaming
  */

/**
--input-topic flinktest --output-topic flinktest_destination
--bootstrap.servers 10.39.43.52:9092,10.39.43.55:9092,10.39.43.56:9092
--zookeeper.connect 10.39.48.189:2181,10.39.48.25:2181,10.39.48.227:2181
--group.id flinktestconsumer
  */
object Kafka010Example {
  def main(args: Array[String]): Unit = {

    val LOG: Logger = LoggerFactory.getLogger(Kafka010Example.getClass)

    //1、parse input arguments
    val params: ParameterTool = ParameterTool.fromArgs(args)
    //2、判断运行条件
    if (params.getNumberOfParameters < 4) {
      println("Missing parameters!\n"
        + "Usage: Kafka --input-topic <topic> --output-topic <topic> "
        + "--bootstrap.servers <kafka brokers> "
        + "--zookeeper.connect <zk quorum> --group.id <some id> [--prefix <prefix>]")
      return
    }

    //3、获取前缀
    val prefix: String = params.get("prefix", "PREFIX:")
    //4、获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //5、废除系统日志
    env.getConfig.disableSysoutLogging()
    //6、配置重启策略
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    //7、create checkpoint every 5 seconds
    env.enableCheckpointing(5000)
    //7、make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)
    //8、create a Kafka streaming source consumer for Kafka 0.10.x
    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010(
      params.getRequired("input-topic"),
      new SimpleStringSchema(),
      params.getProperties)
    //9、创建数据源
    val messageStream = env
      .addSource(kafkaConsumer)
      .map(in=> prefix + in)
//      .map(in=> println(in))

    //10、create a Kafka producer for Kafka 0.10.x
    val kafkaProducer: FlinkKafkaProducer010[String] = new FlinkKafkaProducer010(
      params.getRequired("output-topic"),
      new SimpleStringSchema(),
      params.getProperties
    )

    //11、write data into Kafka
    messageStream.addSink(kafkaProducer)
    //12、执行程序
    LOG.info("Starting job Kafka010Example")
    env.execute("Kafka 0.10 Example")

  }
}
