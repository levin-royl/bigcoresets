package streaming.coresets

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions

import Domain._
import GenUtil._

class CostCalc(sc: SparkContext) extends Serializable {
  def rddName(path: Path): String = {
    path.toUri.toString
  }
  
  def calcCost(
      points: RDD[WPoint], 
      pathToResults: String,
      evalFunc: (Array[Vector], RDD[Vector]) => Double): Unit = {
    
    val before = System.currentTimeMillis
    
    val totalCount = points.count
    var totalNumPoints = 0L

    println(s"indexing ${totalCount} points")
    
//    val data = points.map(p => Vectors.dense(p.toWeightedDoublePoint.getPoint)) // .cache
    val data = points.map(_.toVector).zipWithIndex.materialize.cache
    
    println(s"path\tsampleSize\tnumPoints\ttotal\t#batch\tcost\tk\t|D|\tstats")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val resultsPaths = fs.listStatus(new Path(pathToResults)).map(_.getPath)
    var i = 1
    
    for (path <- resultsPaths) {
      val pathName = rddName(path)
      
      val resRDD = sc.objectFile[ComputedResult](pathName)
      
      if (!resRDD.isEmpty) {
        println(s"now opening RDD from ${pathName}")
        require(resRDD.count == 1L, s"${resRDD.count} != 1L")
  
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
        
        totalNumPoints += res.numPoints
        val cost = evalFunc(res.points, data.filter(_._2 < totalNumPoints).keys)
        
        println(s"$pathName\t${res.sampleSize}\t${res.numPoints}\t${totalNumPoints}/${totalCount}\t$i\t$cost\t${res.points.length}\t${single(res.points.map(_.size))}\t${stats(res.points.map(vec => mean(vec)))}")
        i += 1
      }
      else println(s"skipping empty result ${pathName}")
    }
    
    println(s"done (time = ${(System.currentTimeMillis - before)/1000L} seconds).")
  }
}

object CostCalc extends Serializable {
  def kmeansCost(mat: Array[Vector], data: RDD[Vector]): Double = {
    assert(mat.length > 0 && mat.map(_.size).filter(_ == 0).isEmpty)
    assert(!data.isEmpty)
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