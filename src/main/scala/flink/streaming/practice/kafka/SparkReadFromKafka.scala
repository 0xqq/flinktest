package flink.streaming.practice.kafka

import flink.util.PropertiesUtil
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author:chenchen
  * @Description:
  * @Date:2018 /10/19
  * @Project:flinktest
  * @Package:flink.streaming.practice.kafka
  */
object SparkReadFromKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparktest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val util = new PropertiesUtil("/common.properties")
    val offsetreset = util.getProperty("Dataclean_offsetRest")
    val brokerlist = util.getProperty("brokerlist")
    val zookeeperCon = util.getProperty("zookeeperCon")
    val groupid: String = util.getProperty("Dataclean_groupid")
    val sparkinterval = util.getProperty("Dataclean_sparkinterval").toInt
    val topicStr: String = util.getProperty("Dataclean_topic")
    val ssc = new StreamingContext(sc, Seconds(sparkinterval))
    val topic = topicStr.split(",").toSet
    val kafkaParams = Map("serializer.class" -> "kafka.serializer.StringEncoder", "metadata.broker.list" -> brokerlist, "zookeeper.connect" -> zookeeperCon,
      "auto.offset.reset" -> "smallest", "group.id" -> groupid,
      "zookeeper.session.timeout.ms" -> "40000")
    val kakfaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)
    kakfaStream.transform(rdd=> {
      rdd.mapPartitions(records=> {
        records.map(json => {
          println("i am here")
          json._2
        })
      })
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
