package com.sparkSQL

import org.apache.spark.sql
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder

/**
  * 利用反射机制推断 RDD 模式
  */
package object RDD2DataFrame_Reflect {

    def main(args: Array[String]): Unit = {

        val spark = new sql.SparkSession.Builder().getOrCreate()

        // 支持 RDD 转化为 DataFrame 以及后续的 SQL 操作
        import spark.implicits._

        //声明一个样例类 case class
        case class Person(name : String, age : Int)

        // person.txt
        //      Micheel, 29
        //      Andy, 30
        //      Justin, 19
        val personDF = spark.sparkContext.textFile("file:///person.text")
                .map(_.split(","))
                .map(attributes => Person(attributes(0), attributes(1).trim().toInt)).toDF()

        //      RDD                             DataFrame
        //Person("Micheel", 29)         | "Micheel" | 29  |
        //Person("Andy", 30)        =>  | "Andy"    | 30  |
        //Person("Justin", 19)          | "Justin"  | 19  |

        // 将 personDF 注册为临时表
        personDF.createOrReplaceTempView("people")
        //执行 SQL 查询
        val sqlStr =  """
                     |SELECT name, age
                     |FROM people
                     |WHERE age > 20
                   """.stripMargin

        val peoples = spark.sql(sqlStr) // DataFrame

    }
}
