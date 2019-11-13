package com.xzc.realtime

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * SparkStreaming08_Stop
 *
 * @author xzc
 * @date 2019/11/12
 */
object SparkStreaming_Stop {

  def main(args: Array[String]): Unit = {

    // 优雅的关闭

    // 创建配置对象
    val sparkConf = new SparkConf().setAppName("SparkStreaming01_WordCount").setMaster("local[*]")

    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")

    // 创建SparkStreaming上下文环境对象
    // 构造函数的第二个参数表示数据的采集周期
    val context: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从指定的端口获取数据
    val socketDStream: ReceiverInputDStream[String] = context.socketTextStream("linux1", 9999)

    // 将一行数据进行扁平化操作
    val wordDStream: DStream[String] = socketDStream.flatMap(line => line.split(" "))

    // 将单词转换结构，便于统计
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map {
      word => (word, 1)
    }

    // 将转换后的结构数据进行统计
    val wordToSumDStream: DStream[(String, Int)] = wordToOneDStream.reduceByKey(_ + _)

    // 打印结果
    wordToSumDStream.print()

    // 启动新的线程，希望在特殊的场合关闭SparkStreaming
    new Thread(new Runnable {
      override def run(): Unit = {

        while (true) {
          try {
            Thread.sleep(5000)
          } catch {
            case ex: Exception => println(ex)
          }

          // 监控HDFS文件的变化
          val fs: FileSystem = FileSystem.get(new URI("hdfs://linux1:9000"), new Configuration(), "root")

          val state: StreamingContextState = context.getState()
          // 如果环境对象处于活动状态，可以进行关闭操作
          if (state == StreamingContextState.ACTIVE) {

            // 判断路径是否存在
            val flg: Boolean = fs.exists(new Path("hdfs://linux1:9000/stopSpark1"))
            if (flg) {
              context.stop(true, true)
              System.exit(0)
            }

          }
        }

      }
    }).start()

    // 启动采集器
    context.start()

    // driver等待采集器的结束
    context.awaitTermination()


  }
}
