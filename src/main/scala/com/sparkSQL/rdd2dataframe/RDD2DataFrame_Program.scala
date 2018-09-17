package com.sparkSQL.rdd2dataframe

import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
/**
  * 使用编程方式定义RDD (无法知道数据的结构, 不能提前定义 case class )
  */
object RDD2DataFrame_Program {
    def main(args: Array[String]): Unit = {

        val spark = new sql.SparkSession.Builder().getOrCreate()

        // 支持 RDD 转化为 DataFrame 以及后续的 SQL 操作
        import spark.implicits._

        val peopleRDD = spark.sparkContext.textFile("file:///people.txt")

        // 定义一个模式字符串
        val schemaString = "name age"
        // 根据模式字符串生成模式
        val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
        //Array("name", "age") => Array(StructField("name", StringType, true), StructField("age", StringType, true))

        // 生成模式(包含了 name, age 字段)
        val schema = StructType(fields)

        //将每一行 RDD 文本生成一个 Row 对象和模式进行解析
        val rowRDD = peopleRDD.map(_.split(" ")).map(attribute => Row(attribute(0), attribute(1).trim().toInt)) //Row

        //
        val peopleDF = spark.createDataFrame(rowRDD, schema)// DataFrame[name: String, age: Int]

        // 将 personDF 注册为临时表
        peopleDF.createOrReplaceTempView("people")
        //执行 SQL 查询
        val sqlStr =  """
                        |SELECT name, age
                        |FROM people
                        |WHERE age > 20
                      """.stripMargin

        val peoples = spark.sql(sqlStr) // DataFrame
    }
}
