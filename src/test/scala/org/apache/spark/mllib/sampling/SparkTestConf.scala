package org.apache.spark.mllib.sampling

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkTestConf {
  val conf = new SparkConf()
    .set("spark.app.name", "UnitTest")
    .set("spark.master", "local[2]")

  val sc = new SparkContext(conf)
}
