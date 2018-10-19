package flink.dataclean

import java.io.IOException

import net.sf.json.JSONArray
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicResponseHandler, CloseableHttpClient, HttpClients}
import org.slf4j.LoggerFactory

/**
  * Created by Administrator on 2018/1/15.
  */
object CommonDeploy{
  private val log = LoggerFactory.getLogger(CommonDeploy.getClass)

  def saveToOpenTSDB(rows: Iterator[String], url: String) {
    val requestConfig: RequestConfig = RequestConfig.custom().setSocketTimeout(1000).setConnectTimeout(1000).setConnectionRequestTimeout(1000).build()
    val httpClient: CloseableHttpClient = HttpClients.createDefault
    val jsonArray = new JSONArray
    var summary = 0
    var i = 0
    var count = 0
    val starttime = System.currentTimeMillis()
    // rows foreach前不能调用任何rows的方法,否者下面的foreach不在执行
    rows.foreach(row => {
        summary = summary + row.getBytes.length
        i = i + 1
        count += 1
        if (summary > 8192 || i >= 40) {
          val start = System.currentTimeMillis()
          HttpPostWithJson(url, jsonArray.toString(), requestConfig, httpClient)
          log.info("insert opentsdb jsonArray size: " + jsonArray.size() + " ,summary size :" + summary + ", consume time " + (System.currentTimeMillis() - start))
          jsonArray.clear()

          jsonArray.add(row)
          summary = row.getBytes.length
          i = 1

        } else {
          jsonArray.add(row)
        }
    })
    if (jsonArray.size() > 0) {
      HttpPostWithJson(url, jsonArray.toString(), requestConfig, httpClient)
      log.info("this time cycle end , jsonArray size: " + jsonArray.size() + " ,summary size :" + summary)
    }
    httpClient.close()
    log.info("deal with " + count + " size ,consume time: " + (System.currentTimeMillis() - starttime))
  }


  def HttpPostWithJson(url: String, json: String, requestConfig: RequestConfig, httpClient: CloseableHttpClient): Boolean = {
    //    var tsdbUrl = url
    var jsonarr = json
    val maxTryTime = 5
    var tryTimes = 0
    var success = false
    while (!success) {
      log.info(s"get opentsdb url : ${url}")
      tryTimes += 1
      try {
        //第二步：创建httpPost对象
        val httpPost = new HttpPost(url)
        val responseHandler = new BasicResponseHandler
        //第三步：给httpPost设置JSON格式的参数
        val requestEntity = new StringEntity(jsonarr, "UTF-8")
        requestEntity.setContentType("text/plain")
        requestEntity.setContentEncoding("UTF-8")
        httpPost.addHeader("Content-type", "text/plain; charset=utf-8")
        httpPost.setHeader("Accept", "text/plain")
        httpPost.setConfig(requestConfig)
        httpPost.setEntity(requestEntity)
        // 第四步：发送HttpPost请求，获取返回值
        httpClient.execute(httpPost, responseHandler) // 调接口获取返回值时，必须用此方法
        success = true
      } catch {
        case e: Exception =>
          success = false
          try {
            var newJson = ""
            val oldstr=jsonarr.replace(" ","").replace("\u0000","").replace("u0000","").replace("\\","").replace("\"{","{").replace("}\"","}")
            val jsonArray: JSONArray = JSONArray.fromObject(oldstr)
            if (jsonArray.size() > 0) {
              for (i <- 0 until jsonArray.size()) {
                val objectStr=jsonArray.getJSONObject(i)
                val single = objectStr.toString
                val value=objectStr.get("value").toString
                val singleVer: Boolean = single.matches("\\{\"metric\":\"[a-z|A-Z|0-9]+((\\.)?[a-z|A-Z|0-9]+)+\",\"value\":\"-?[0-9]+(.[0-9]+)?\",\"timestamp\":\"[0-9]{13}\",\"tags\":\\{\"[a-z|A-Z|0-9|.|_]+\":\"[a-z|A-Z|0-9|.|_]+\"(,\"[a-z|A-Z|0-9|.|_]+\":\"[a-z|A-Z|0-9|.|_]+\")*}}")
                if (singleVer && value.toDouble<=Long.MaxValue.toDouble && value.toDouble>=Long.MinValue.toDouble){
                    newJson += single + ","
                } else {
                  log.error(single, e)
                }
              }
              if (!newJson.equals("")) {
                jsonarr = "[" + newJson.substring(0, newJson.lastIndexOf(",")) + "]"
              } else {
                success = true
                tryTimes = 0
                log.error("数组存在异常数据:"+json, e)
              }
            }
          } catch {
            case e: Exception =>
              log.error("Exception occurs when an error string is handled!", e)
          }
          if (tryTimes > maxTryTime) {
            log.error("尝试5次后失败，异常数据:"+json+"\n处理后的数据："+jsonarr, e)
            throw new IOException(s"exception data ${json} ,after try ${maxTryTime} times, save to opentsdb fail!!")
          }
      }
    }
    // 第五步：处理返回值
    success
  }

}