package com.sparkSQL.jdbc

import java.util.Properties

import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * 读写 Mysql 中的数据
  */
object JDBC_RW {
    def main(args: Array[String]): Unit = {
        val spark = new sql.SparkSession.Builder().master("local[2]").getOrCreate()

        // 启动隐式转换 支持 RDD 转化为 DataFrame 以及后续的 SQL 操作
        import spark.implicits._

        //从 mysql 中读取数据
        val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306/sql_text")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "course")
                .option("user", "root")
                .option("password", "123")
                .load()

        jdbcDF.show()

        // 向 mysql 中写入数据
        // 设置插入数据
        val studentRDD = spark.sparkContext.parallelize(Array("006 机器学习 4", "007 软件工程 5")).map(_.split(" "))

        // 生成模式信息
        val fields = "cid cName tid".split(" ").map(fileName => StructField(fileName, StringType, nullable = true))
        val schema = StructType(fields)

        // 生成 Row 对象
        val rowVaue = studentRDD.map(line => Row(line(0).trim, line(1).trim, line(2).trim))

        // 挂接生成 DF
        val studentDF = spark.createDataFrame(rowVaue, schema) // DF

        //定义 properties 保存 jdbc 参数
        val prop = new Properties()
        prop.put("driver", "com.mysql.jdbc.Driver")
        prop.put("user", "root")
        prop.put("password", "123")

        // 保存数据到 mysql
        studentDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/sql_text", "course", prop)

    }
}
