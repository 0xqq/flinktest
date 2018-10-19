package flink.streaming.practice.kafka

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import flink.util.PropertiesUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.slf4j.{Logger, LoggerFactory}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /10/19
  * @Project:flinktest
  * @Package:flink.streaming.practice.kafka
  */

//--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
//--topic flinktest02 --bootstrap.servers 10.39.43.52:9092,10.39.43.55:9092,10.39.43.56:9092 --zookeeper.connect 10.39.48.189:2181,10.39.48.25:2181,10.39.48.227:2181 --group.id myGroup
object FlinkReadFromKafka {

  private val log: Logger = LoggerFactory.getLogger(FlinkReadFromKafka.getClass)

  //kafka info
  var zkCluster = ""
  var kafkaCluster = ""
  var topic = ""
  var groupid = ""

  def main(args: Array[String]): Unit = {


    //flink env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //configure flink env
//    env.getConfig.disableSysoutLogging()
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000))
    env.enableCheckpointing(5000)

    //create kafkastream
    val util = new PropertiesUtil("/common.properties")
    kafkaCluster = util.getProperty("brokerlist")
    zkCluster = util.getProperty("zookeeperCon")
    topic = util.getProperty("Dataclean_topic")
    groupid = util.getProperty("Dataclean_groupid")

    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaCluster)
    props.setProperty("zookeeper.connect", zkCluster)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("group.id", groupid)

//    env.getConfig.setGlobalJobParameters(util)

    //create stream, 此处用 FlinkKafkaConsumer010
    val messageStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer010[String](
      topic,
      new SimpleStringSchema(),
      props
    ))

    messageStream.rebalance.map(json=> {
//      val jsonObj: JSONObject = JSON.parseObject(json)
//      println(jsonObj)
//      val value = jsonObj.get("Message")
      val list = scala.collection.mutable.MutableList[String]()
      try {
        val jsonObj = JSON.parseObject(json)
        val staId = jsonObj.getString("staId")
        val domainMK = jsonObj.getString("domain")
        val jsonArray = jsonObj.getJSONArray("data")
        val it = jsonArray.iterator()
        while (it.hasNext()) {
          val inner = it.next().asInstanceOf[JSONObject]
          val metric = inner.getString("metric")
          if (!metric.trim().isEmpty()) {
            try {
              val value = inner.getString("value")
              val timestamp = inner.getString("time")
              val splits = metric.split("_")
              if (splits.length > 1) {
                var tagStr=""
                val tag1 = inner.get("tag1")
                if(tag1 !=null){
                  tagStr=tagStr+"\"tag1\":\""+tag1+"\","
                }
                val tag2 = inner.get("tag2")
                if(tag2 !=null){
                  tagStr=tagStr+"\"tag2\":\""+tag2+"\","
                }
                val tag3 = inner.get("tag3")
                if(tag3 !=null){
                  tagStr=tagStr+"\"tag3\":\""+tag3+"\","
                }
                val tag4 = inner.get("tag4")
                if(tag4 !=null){
                  tagStr=tagStr+"\"tag4\":\""+tag4+"\","
                }
                val tag5 = inner.get("tag5")
                if(tag5 !=null){
                  tagStr=tagStr+"\"tag5\":\""+tag5+"\","
                }
                val tag6 = inner.get("tag6")
                if(tag6 !=null){
                  tagStr=tagStr+"\"tag6\":\""+tag6+"\","
                }
                val equipMK = splits(0)
                val equipID = splits(1)
                val AttriMK = splits(2)
                var returnstr =""
                if(tagStr.equals("")){
                  returnstr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",\"tags\":{\"staId\":\"" + staId + "\"" + ",\"equipMK\":\"" + equipMK + "\",\"equipID\":\"" + equipID + "\"}}"
                }else{
                  returnstr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",\"tags\":{\"staId\":\"" + staId + "\"" + ",\"equipMK\":\"" + equipMK + "\",\"equipID\":\"" + equipID + "\""+","+tagStr.substring(0,tagStr.length-1)+"}}"
                }
                if (!value.toString.contains("NaN")) {
                  list += returnstr
                } else {
                  if (value.toString.contains("NaN")) {
                    log.error("数据包存在异常数据：{}\n" + returnstr.replaceAll("\\", "") + "\n")
                  } else {
                    log.error("数据包存在异常数据：{}\n" + returnstr + "\n")
                  }
                }
              }
            } catch {
              case e: Exception =>
                log.error("数据包解析时异常：{}\n" + inner + "\n", e)
                e.printStackTrace()
            }
          }
        }
      }
      catch {
        case e: Exception =>
          log.error("从kafka取数时报错{}\n" + json, e)
          e.printStackTrace()
      }
      list
    }
    )
    env.execute()
  }
}
