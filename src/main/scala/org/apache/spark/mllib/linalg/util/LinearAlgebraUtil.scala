package org.apache.spark.mllib.linalg.util

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors

object LinearAlgebraUtil extends Serializable {
  implicit class MatrixHelper(mat: Matrix) {
    def row(i: Int): Vector = {
      val values = (0 until mat.numCols).map(j => mat(i, j))
      Vectors.dense(values.toArray)
    }
    
    def rows(): Seq[Vector] = {
      (0 until mat.numRows).map(row)
    }
  }
}