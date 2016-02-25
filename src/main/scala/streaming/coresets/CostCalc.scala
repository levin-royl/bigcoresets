package streaming.coresets

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.DenseMatrix
import Domain._

class CostCalc(sc: SparkContext) extends Serializable {
  def calcCost(
      points: RDD[WPoint], 
      pathToResults: String,
      evalFunc: (Array[Vector], RDD[Vector]) => Double): Unit = {
    val before = System.currentTimeMillis
    
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val resultsPaths = fs.listStatus(new Path(pathToResults)).map(_.getPath.toUri.toString)

    var i = 1
//    val data = points.map(p => Vectors.dense(p.toWeightedDoublePoint.getPoint)) // .cache
    val data = points.map(p => Vectors.dense(p.getPoint)).cache

    println(s"path\t#batch\tcost\tk\t|D|\tstats")
      
    for (path <- resultsPaths) {
      val resRDD = sc.objectFile[Array[Vector]](path)
      require(resRDD.count == 1L)

      val res = resRDD.first
      
      def mean(vec: Vector) = {
        val arr = vec.toArray
        arr.sum/arr.length
      }
      
      def stats(iter: Iterable[Double]) = sc.makeRDD(iter.toSeq).stats
      
      def single(iter: Iterable[Int]) = {
        val set = iter.toSet
        require(set.size == 1)
        set.iterator.next
      }
      
      val cost = evalFunc(res, data)
      
      println(s"$path\t$i\t$cost\t${res.length}\t${single(res.map(_.size))}\t${stats(res.map(vec => mean(vec)))}")
      i += 1
    }
    
    println(s"done (time = ${(System.currentTimeMillis - before)/1000L} seconds).")
  }
}

object CostCalc extends Serializable {
  def kmeansCost(mat: Array[Vector], data: RDD[Vector]): Double = {
    val kmeansModel = new KMeansModel(mat)
    kmeansModel.computeCost(data)
  }
  
  def svdCost(mat: Array[Vector], data: RDD[Vector]): Double = {
    val A = new RowMatrix(data)
    val VT = new DenseMatrix(mat.length, mat(0).size, mat.flatMap(_.toArray))
    val V = VT.transpose
    
    val costMat = A.multiply(V.multiply(VT))
    
    // http://mathworld.wolfram.com/FrobeniusNorm.html
    Math.sqrt(costMat.rows.flatMap(_.toArray.map(v => v*v)).sum)
  }  
}