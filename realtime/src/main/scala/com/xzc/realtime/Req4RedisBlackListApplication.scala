package com.xzc.realtime

import java.util

import com.xzc.common.model.MyKafkaMessage
import com.xzc.common.util.{DateUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req4RedisBlackListApplication {

  def main(args: Array[String]): Unit = {

    // 需求四 ： 广告黑名单实时统计

    // 准备配置对象
    val sparkConf = new SparkConf().setAppName("Req4BlackListApplication").setMaster("local[*]")

    // 构建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //StreamingContext.getActiveOrCreate()

    ssc.sparkContext.setCheckpointDir("cp")

    // 采集kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("ads_log", ssc)

    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val message = record.value()
      val datas: Array[String] = message.split(" ")
      MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // Coding : Driver 1
    val transformDStream: DStream[MyKafkaMessage] = messageDStream.transform(rdd => {
      // Coding : Driver n
      val client: Jedis = RedisUtil.getJedisClient
      //val client: Jedis = new Jedis("linux4", 6379)
      val blackList: util.Set[String] = client.smembers("blacklist")
      // Driver => object => Driver
      //blackList.contains("1")
      // 使用广播变量来实现序列化操作
      val blackListBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackList)

      client.close()

      rdd.filter(message => {
        // Coding : Executor m
        // Driver ==> Executor
        !blackListBroadcast.value.contains(message.userid)
      })
    })

    // TODO 1. 将数据进行结构的转换 ：(date-adv-user, 1)
    val dateAdvUserToCountDStream: DStream[(String, Long)] = transformDStream.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.adid + "_" + message.userid, 1L)
    })


    // TODO 3. 对聚合的结果进行阈值的判断
    dateAdvUserToCountDStream.foreachRDD(rdd => {

      rdd.foreachPartition(datas => {
        val innerClient: Jedis = RedisUtil.getJedisClient
        //val innerClient: Jedis = new Jedis("linux4", 6379)

        datas.foreach {
          case (key, sum) => {
            // 向redis中更新数据
            innerClient.hincrBy("date:adv:user:click", key, 1)
            // 获取当前最新的值进行判断
            val clickCount: String = innerClient.hget("date:adv:user:click", key)
            if (clickCount.toInt >= 100) {
              val keys: Array[String] = key.split("_")
              innerClient.sadd("blacklist", keys(2))
            }
          }
        }

        innerClient.close()
      })

      /*
      rdd.foreach{
          case ( key, sum ) => {
              val innerClient: Jedis = RedisUtil.getJedisClient
              // 向redis中更新数据
              innerClient.hincrBy("date:adv:user:click", key, 1)
              // 获取当前最新的值进行判断
              val clickCount: String = innerClient.hget("date:adv:user:click", key)
              if (clickCount.toInt >= 100) {
                  val keys: Array[String] = key.split("_")
                  innerClient.sadd("blacklist", keys(2))
              }

              innerClient.close()
          }
      }
      */
    })

    ssc.start()

    ssc.awaitTermination()

  }
}
