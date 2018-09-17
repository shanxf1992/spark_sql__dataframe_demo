package com.sparkSQL.json

import org.apache.spark.sql
/**
  * sparkSQL 处理json 数据
  */
object ReadAndWriteRDD {
    def main(args: Array[String]): Unit = {

        val spark = new sql.SparkSession.Builder().getOrCreate()

        // 支持 RDD 转化为 DataFrame 以及后续的 SQL 操作
        import spark.implicits._

        val df = spark.read.json("file:///people.json") // org.apache.spark.sql.DataFrame

        val peopleDF = df.select("name", "age")
        peopleDF.write.mode("overwrite").format("csv").save("/people") // 保存 DataFrame
        // peopleDF.rdd.saveAsTextFile("/people")
    }
}
