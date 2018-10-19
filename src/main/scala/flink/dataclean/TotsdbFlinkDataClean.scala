//package flink.dataclean
//
//import java.util.Properties
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.api.java.utils.ParameterTool
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
//import org.slf4j.LoggerFactory
//
///**
//  * @Author:chenchen
//  * @Description:
//  * @Date:2018 /10/18
//  * @Project:flinktest
//  * @Package:flink.dataclean
//  */
//object TotsdbFlinkDataClean {
//
//  private val log = LoggerFactory.getLogger(TotsdbFlinkDataClean.getClass)
//  var brokerlist: String = ""
//  var zookeeperCon: String = ""
//  var checkpointInterval: Int = 0
//
//  def main(args: Array[String]): Unit = {
//
////    val util = new PropertiesUtil("common.properties")
////    checkpointInterval = util.getProperty("Dataclean_checkpoint_interval").toInt
//    checkpointInterval = 5000
////    val params: ParameterTool = ParameterTool.fromArgs(args)
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val util = new PropertiesUtil("common.properties")
//    val topicStr = util.getProperty("Dataclean_topic")
//    val topics = topicStr.split(",").toSet
//    brokerlist = util.getProperty("brokerlist")
//    zookeeperCon = util.getProperty("zookeeperCon")
//    val props = new Properties()
//
//
//
//
//
////    env.getConfig.disableSysoutLogging()
//    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000)) //重启策略
//    env.enableCheckpointing(checkpointInterval) //配置检查点间隔
////    env.getConfig.setGlobalJobParameters(params) //使参数全局有效
//    val kafkaConsumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010(
//      topics,
//      new SimpleStringSchema(),
//      params.getProperties
//    )
//    val messageStream: DataStream[String] = env.addSource(kafkaConsumer)
//
//    val kafkaProducer = new FlinkKafkaProducer010(
//      params.getRequired("output-topic"),
//      new SimpleStringSchema(),
//      params.getProperties
//    )
//
//    messageStream.addSink(kafkaProducer)
//
//    env.execute()
////    env.execute("Flinktest")
//  }
//
//}
