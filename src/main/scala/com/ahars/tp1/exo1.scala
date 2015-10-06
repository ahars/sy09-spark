package com.ahars.tp1

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object exo1 {

  def main(args: Array[String]) {

    val schema = StructType(Array(
      StructField("bwt", DataTypes.IntegerType),
      StructField("gestation", DataTypes.IntegerType),
      StructField("parity", DataTypes.IntegerType),
      StructField("age", DataTypes.IntegerType),
      StructField("height", DataTypes.IntegerType),
      StructField("weight", DataTypes.IntegerType),
      StructField("smoke", DataTypes.IntegerType),
      StructField("education", DataTypes.IntegerType)
    ))

    val conf = new SparkConf()
      .setAppName("sy09-spark-tp1-exo1")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val lines = sc.textFile("src/main/resources/tp1/babies23.txt")
      .map(line => line.dropWhile(_ == ' '))
      .filter(line => !line.startsWith("id"))
      .map(line => line.replaceAll("\\s+", ";"))
      .map(_.split(";"))

    val rows = lines.map(fields => Row(fields(6).toInt, fields(4).toInt, fields(7).toInt, fields(9).toInt,
      fields(11).toInt, fields(12).toInt, fields(20).toInt, fields(10).toInt
    ))

    val df = sqlc.createDataFrame(rows, schema)

    


  }
}
