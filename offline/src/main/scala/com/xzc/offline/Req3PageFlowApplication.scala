package com.xzc.offline

import com.xzc.common.model.UserVisitAction
import com.xzc.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// 需求3 ： 页面单跳转化率统计
object Req3PageFlowApplication {

  def main(args: Array[String]): Unit = {


    // 准备上下文环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req2CategoryTop10SessionTop10Application")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    sparkSession.sparkContext.setCheckpointDir("cp")

    import sparkSession.implicits._

    sparkSession.sql("use " + ConfigUtil.getValueFromConfig("hive.database"))

    var sql = "select * from user_visit_action where 1 = 1 "

    val startDate = ConfigUtil.getValueFromCondition("startDate")
    val endDate = ConfigUtil.getValueFromCondition("endDate")

    if (StringUtil.isNotEmpty(startDate)) {
      sql = sql + " and date >='" + startDate + "'"
    }

    if (StringUtil.isNotEmpty(endDate)) {
      sql = sql + " and date <='" + endDate + "'"
    }

    val df: DataFrame = sparkSession.sql(sql)
    val ds: Dataset[UserVisitAction] = df.as[UserVisitAction]
    val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd

    // 持久化RDD
    userVisitActionRDD.checkpoint()

    // ********************** 需求三 start **********************************

    // TODO 计算分母数据
    // TODO 4.1 从行为表中获取数据（pageid）
    // TODO 4.2 对数据进行筛选过滤，保留需要统计的页面数据

    // 1,2,3,4,5,6,7
    // 1-2,2-3,3-4,4-5,5-6,6-7
    val pageids: Array[String] = ConfigUtil.getValueFromCondition("targetPageFlow").split(",")

    // 使用拉链将数据关联在一起
    val pageid1AndPageid2Array: Array[String] = pageids.zip(pageids.tail).map {
      case (pageid1, pageid2) => {
        pageid1 + "-" + pageid2
      }
    }

    val pageRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(action => {
      pageids.contains(action.page_id.toString)
    })

    // TODO 4.3 将页面数据进行结构的转换（pageid, 1）
    val pageToCountRDD: RDD[(Long, Long)] = pageRDD.map(action => {
      (action.page_id, 1L)
    })

    // TODO 4.4 将转换后的数据进行聚合统计（pageid, sum）(分母)
    val pageToSumRDD: RDD[(Long, Long)] = pageToCountRDD.reduceByKey(_ + _)

    val pageMap: Map[Long, Long] = pageToSumRDD.collect().toMap

    // TODO 计算分子数据
    // TODO 4.5 从行为表中获取数据，使用session进行分组（sessionid, Iterator[ (pageid, action_time)  ]）
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action => {
      action.session_id
    })

    // TODO 4.6 对分组后的数据进行时间排序（升序）
    // TODO 4.7 将排序后的页面ID，两两结合：（1-2，2-3，3-4）
    val pageidZipRDD: RDD[List[(Long, Long)]] = sessionGroupRDD.map {
      case (session, datas) => {
        val actions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }

        // 1,2,3,4,5
        // 2,3,4,5
        val pageids: List[Long] = actions.map(action => {
          action.page_id
        })
        // zip
        //pageids.sliding()
        // 使用拉链实现两两结合的效果
        val pageidZipList: List[(Long, Long)] = pageids.zip(pageids.tail)
        pageidZipList
      }
    }

    val pageidZipFlatRDD: RDD[(Long, Long)] = pageidZipRDD.flatMap(list => list)

    // TODO 4.8 对数据进行筛选过滤（1-2，2-3，3-4，4-5，5-6，6-7）
    val filterRDD: RDD[(Long, Long)] = pageidZipFlatRDD.filter(pageidZip => {
      pageid1AndPageid2Array.contains(pageidZip._1 + "-" + pageidZip._2)
    })

    // TODO 4.9 对过滤后的数据进行结构的转换：（pageid1-pageid2, 1）
    val pageFlowToCountRDD: RDD[(String, Long)] = filterRDD.map {
      case (pageid1, pageid2) => {
        (pageid1 + "-" + pageid2, 1L)
      }
    }

    // TODO 4.10 对转换结构后的数据进行聚合统计：（pageid1-pageid2, sum1）(分子)
    val pageFlowToSumRDD: RDD[(String, Long)] = pageFlowToCountRDD.reduceByKey(_ + _)

    // TODO 4.11 查询对应的分母数据 （pageid1, sum2）
    // TODO 4.12 计算转化率 ： sum1 / sum2
    pageFlowToSumRDD.foreach {
      case (pageflow, sum) => {
        // 分子 1-2, sum1
        val pageid1 = pageflow.split("-")(0)

        // 获取分母 1, sum2
        val sum2: Long = pageMap.getOrElse(pageid1.toLong, 1L)

        // sum1 / sum2
        println(pageflow + "=" + (sum.toDouble / sum2))
      }
    }

    // ********************** 需求三 end **********************************

    // 释放资源
    sparkSession.close()

  }
}