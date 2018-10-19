//package flink.dataclean
//
//import com.alibaba.fastjson.{JSON, JSONObject}
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.spark.streaming.kafka._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.slf4j.LoggerFactory
//
///**
//  * 泛能数据补招
//  * Created by Administrator on 2017/11/28.
//  */
//
//object TotsdbDataClean{
//  private val log = LoggerFactory.getLogger(TotsdbDataClean.getClass)
//  var brokerlist: String = ""
//  var zookeeperCon: String = ""
//  var appname: String = ""
//  var times: Int = 1
//  var totalNum: Long = 0L
//  //总历史数据量
//  var totalMem: Double = 0 //总历史数据量，多少G
//
//  def main(args: Array[String]): Unit = {
//    initSpark()
//  }
//
//  /**
//    * 保存数据
//    * kafka生产者发送数据保存opentsdb
//    */
//  def initSpark(): Unit = {
//    try {
//      val util: PropertiesUtil = new PropertiesUtil("/common.properties")
//      val topicStr = util.getProperty("Dataclean_topic")
//      val groupid = util.getProperty("Dataclean_groupid")
//      val offsetRest = util.getProperty("Dataclean_offsetRest")
//      appname = util.getProperty("Dataclean_appname")
//      times = util.getProperty("Dataclean_times").toInt
//
//      brokerlist = util.getProperty("brokerlist")
//      zookeeperCon = util.getProperty("zookeeperCon")
//      val topics = topicStr.split(",").toSet
//      val conf = new SparkConf().setAppName(appname)
//                    .setMaster("local[1]")
//      val sc = new SparkContext(conf)
//      //设置1秒从kafka中拉取数据
//      val ssc = new StreamingContext(sc, Seconds(times))
//      //      val accum = sc.accumulator(0, "OpentsDB Selector")
//      var kafkaParams: Map[String, String] = null
//      kafkaParams = Map("serializer.class" -> "kafka.serializer.StringEncoder", "metadata.broker.list" -> brokerlist, "zookeeper.connect" -> zookeeperCon,
//        "auto.offset.reset" -> offsetRest, "group.id" -> groupid,
//        "zookeeper.session.timeout.ms" -> "40000")
//      //smallest，largest
//      try {
//        //  获取offset
//        val fromOffsets = KafkaManager.getFromOffset(kafkaParams, topics)
//        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
//        val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
//        var offsetRanges: Array[OffsetRange] = null
//        val trans = kafkaStream.transform(rdd => {
//          rdd.count()
//          val retrdd = rdd.mapPartitions(records => {
//            val ret = records.map(pair => {
//              val list = scala.collection.mutable.MutableList[String]()
//              val json = pair._2
//              try {
//                val jsonObj = JSON.parseObject(json)
//                val staId = jsonObj.getString("staId")
//                val domainMK = jsonObj.getString("domain")
//                val jsonArray = jsonObj.getJSONArray("data")
//                val it = jsonArray.iterator()
//                while (it.hasNext()) {
//                  val inner = it.next().asInstanceOf[JSONObject]
//                  val metric = inner.getString("metric")
//                  if (!metric.trim().isEmpty()) {
//                    try {
//                      val value = inner.getString("value")
//                      val timestamp = inner.getString("time")
//                      val splits = metric.split("_")
//                      if (splits.length > 1) {
//                        var tagStr=""
//                        val tag1 = inner.get("tag1")
//                        if(tag1 !=null){
//                          tagStr=tagStr+"\"tag1\":\""+tag1+"\","
//                        }
//                        val tag2 = inner.get("tag2")
//                        if(tag2 !=null){
//                          tagStr=tagStr+"\"tag2\":\""+tag2+"\","
//                        }
//                        val tag3 = inner.get("tag3")
//                        if(tag3 !=null){
//                          tagStr=tagStr+"\"tag3\":\""+tag3+"\","
//                        }
//                        val tag4 = inner.get("tag4")
//                        if(tag4 !=null){
//                          tagStr=tagStr+"\"tag4\":\""+tag4+"\","
//                        }
//                        val tag5 = inner.get("tag5")
//                        if(tag5 !=null){
//                          tagStr=tagStr+"\"tag5\":\""+tag5+"\","
//                        }
//                        val tag6 = inner.get("tag6")
//                        if(tag6 !=null){
//                          tagStr=tagStr+"\"tag6\":\""+tag6+"\","
//                        }
//                        val equipMK = splits(0)
//                        val equipID = splits(1)
//                        val AttriMK = splits(2)
//                        var returnstr =""
//                        if(tagStr.equals("")){
//                         returnstr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",\"tags\":{\"staId\":\"" + staId + "\"" + ",\"equipMK\":\"" + equipMK + "\",\"equipID\":\"" + equipID + "\"}}"
//                        }else{
//                          returnstr = "{\"metric\":\"" + domainMK + "." + AttriMK + "\",\"value\":\"" + value + "\",\"timestamp\":\"" + timestamp + "\",\"tags\":{\"staId\":\"" + staId + "\"" + ",\"equipMK\":\"" + equipMK + "\",\"equipID\":\"" + equipID + "\""+","+tagStr.substring(0,tagStr.length-1)+"}}"
//                        }
//                        if (!value.toString.contains("NaN")) {
//                          list += returnstr
//                        } else {
//                          if (value.toString.contains("NaN")) {
//                            log.error("数据包存在异常数据：{}\n" + returnstr.replaceAll("\\", "") + "\n")
//                          } else {
//                            log.error("数据包存在异常数据：{}\n" + returnstr + "\n")
//                          }
//                        }
//                      }
//                    } catch {
//                      case e: Exception =>
//                        log.error("数据包解析时异常：{}\n" + inner + "\n", e)
//                        e.printStackTrace()
//                    }
//                  }
//                }
//              } catch {
//                case e: Exception =>
//                  log.error("从kafka取数时报错{}\n" + json, e)
//                  e.printStackTrace()
//              }
//              list
//            })
//            ret
//          })
//          offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//          retrdd
//        }).flatMap { list => {
//          list
//        }
//        }
//
//        val url = util.getProperty("puturl2")
//        trans.foreachRDD(rdd => {
//          if (rdd.count() > 0) {
//            rdd.foreachPartition { rows => {
//              CommonDeploy.saveToOpenTSDB(rows, url)
//            }
//            }
//            val kc = new KafkaManager().makeKafkaCluster(kafkaParams)
//            KafkaManager.updateOffset(kc, kafkaParams.get("group.id").get, offsetRanges)
//          } else {
//            log.info(s"rdd is empty")
//          }
//        }
//
//        )
//      } catch {
//        case e: Exception =>
//          log.error("从kafka取数时报错{}", e)
//          e.printStackTrace()
//      }
//      ssc.start
//      ssc.awaitTermination
//    } catch {
//      case e: Exception =>
//        log.error("执行spark程序从kafka取数时异常", e)
//    }
//  }
//}
