package org.apache.spark.mllib.sampling

import org.apache.spark.rdd.RDD

object TestUtils {
  implicit class TestHelperRDD[T](rdd: RDD[T]) {
    def single(): T = {
      require(rdd.count == 1L)
      rdd.first
    }
  }
  
  implicit class TestHelperRDDLike[T](rdd: RDDLike[T]) {
    def single(): T = {
      require(rdd.count == 1L)
      rdd.first
    }
  }
}
