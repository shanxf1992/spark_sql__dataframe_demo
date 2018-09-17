package com.sparkSQL.parquet

import org.apache.spark.sql

/**
  * Parquet : (链式存储格式)仅仅是一种存储格式，它是语言、平台无关的，并且不需要和任何一种数据处理框架绑定
  */
object ParquetRW {
    def main(args: Array[String]): Unit = {
        val spark = new sql.SparkSession.Builder().getOrCreate()

        // 启动隐式转换 支持 RDD 转化为 DataFrame 以及后续的 SQL 操作
        import spark.implicits._

        // 从 parquet 文件中读取数据 生成 DataFrame
        val parquetDF = spark.read.parquet("file:///users.parquet")

        //注册临时表
        parquetDF.createOrReplaceTempView("parquet")

        //执行 SQL 查询
        val sqlStr =  """
                        |SELECT name
                        |FROM parquet
                      """.stripMargin

        val namesDF = spark.sql(sqlStr) // DataFrame

        //保存 DataFrame 为 parquet 数据
        namesDF.write.parquet("/parquet")
    }
}
