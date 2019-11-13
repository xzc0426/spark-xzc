package com.xzc.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.xzc.common.model.UserVisitAction
import com.xzc.common.util.{ConfigUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{immutable, mutable}

object Req1CategoryTop10Application {

  def main(args: Array[String]): Unit = {

    // TODO 需求1 ： 获取点击、下单和支付数量排名前 10 的品类

    // 准备上下文环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    // TODO 4.1 从Hive表中获取用户行为数据
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
    //println(userVisitActionRDD.count())

    // TODO 4.2 使用累加器将不同的品类的不同指标数据聚合在一起 ： （K-V）(category-指标, SumCount)
    val accumulator = new CategoryCountAccumulator
    sparkSession.sparkContext.register(accumulator)

    userVisitActionRDD.foreach(action => {
      if (action.click_category_id != -1) {
        accumulator.add(action.click_category_id + "-click")
      } else if (action.order_category_ids != null) {
        val ids = action.order_category_ids.split(",")
        for (id <- ids) {
          accumulator.add(id + "-order")
        }
      } else if (action.pay_category_ids != null) {
        val ids = action.pay_category_ids.split(",")
        for (id <- ids) {
          accumulator.add(id + "-pay")
        }
      }
    })

    // TODO 4.3 将聚合后的结果转化结构：(category-指标, SumCount) (category,(指标, SumCount))
    val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value
    val categoryToCountMap: mutable.HashMap[String, (String, Long)] = categoryCountMap.map {
      case (key, count) => {
        val keys = key.split("-")
        (keys(0), (keys(1), count))
      }
    }

    // TODO 4.4 将转换结构后的相同品类的数据分组在一起
    val groupByMap: Map[String, mutable.HashMap[String, (String, Long)]] = categoryToCountMap.groupBy {
      case (k, v) => k
    }
    groupByMap.foreach(println)

    // map (10 -> (click, 30))
    // map (10 -> (order, 20))
    // map (10 -> (pay, 10))
    val categorys: immutable.Iterable[CategoryTop10] = groupByMap.map {
      case (category, map) => {

        var clickCount = 0L
        var orderCount = 0L
        var payCount = 0L

        val maybeTuple: Option[(String, Long)] = map.get(category)
        val t = maybeTuple.get

        if ("click".equals(t._1)) {
          clickCount = t._2
        } else if ("order".equals(t._1)) {
          orderCount = t._2
        } else {
          payCount = t._2
        }


        CategoryTop10(UUID.randomUUID().toString, category, clickCount, orderCount, payCount)
      }
    }

    // TODO 4.5 根据品类的不同指标进行排序（降序）
    val sortedList: List[CategoryTop10] = categorys.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }
    // TODO 4.6 获取排序后的前10名
    val top10List: List[CategoryTop10] = sortedList.take(10)

    // TODO 4.7 将结果保存到数据库中
    val driver = ConfigUtil.getValueFromConfig("jdbc.driver.class")
    val url = ConfigUtil.getValueFromConfig("jdbc.url")
    val user = ConfigUtil.getValueFromConfig("jdbc.user")
    val password = ConfigUtil.getValueFromConfig("jdbc.password")

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url, user, password)
    val sqlString = "insert into category_top10 values (?,?,?,?,?)"
    val statement: PreparedStatement = connection.prepareStatement(sqlString)

    top10List.foreach(data => {
      statement.setString(1, data.taskId)
      statement.setString(2, data.categoryId)
      statement.setLong(3, data.clickCount)
      statement.setLong(4, data.orderCount)
      statement.setLong(5, data.payCount)
      statement.executeUpdate()
    })

    statement.close()
    connection.close()

    // 释放资源
    sparkSession.close()

  }
}

case class CategoryTop10(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long)

// 声明累加器
class CategoryCountAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value

    map = map1.foldLeft(map2) {
      (innerMap, kv) => {

        // map访问操作
        val k = kv._1
        val v = kv._2

        // 更新map中key的值
        innerMap(k) = innerMap.getOrElse(k, 0L) + v

        innerMap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}