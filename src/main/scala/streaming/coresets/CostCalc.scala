package streaming.coresets

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.clustering.KMeansModel
import Domain._

object CostCalc extends Serializable {
  val centerNames = "bulk-nonuniform-centers_131072_2"
//  val centerNames = "spark-bulk-center"
  
//  val centerNames = "nonuniform-centers_4096_4"
//  val centerNames = "uniform-centers"
//  val centerNames = "spark-centers"
  
  def parse = parseSparse _
//  def parse = parseDense _

  def main(args: Array[String]) {
    val before = System.currentTimeMillis
    
    require(!args.isEmpty)
    
    val dataFile = args(0)
    val rootDir = if (args.length > 1) args(1) else System.getProperty("user.home")
    
    val sparkLocalDir = s"${rootDir}/spark-temp/local"

    val sparkConf = new SparkConf()
      .setAppName("KMeansCostCalculator")
      .setMaster(s"local[16]")
      .set("spark.local.dir", sparkLocalDir)
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")

    val sc = new SparkContext(sparkConf)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val centerPaths = fs.listStatus(new Path(s"$sparkLocalDir/$centerNames")).map(_.getPath.toUri.toString)

    var i = 1
    val data = sc.textFile(dataFile).map(line => Vectors.dense(parse(line).getPoint)) // .cache

    println(s"path\t#batch\tcost\tk\t|D|\tstats")
      
    for (centerPath <- centerPaths) {
      val centersRDD = sc.objectFile[Array[Vector]](centerPath)
      require(centersRDD.count == 1L)

      val centers = centersRDD.first
      val kmeansModel = new KMeansModel(centers)
      
      val cost = kmeansModel.computeCost(data)
      
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
      
      println(s"$centerPath\t$i\t$cost\t${centers.length}\t${single(centers.map(_.size))}\t${stats(centers.map(vec => mean(vec)))}")
      i += 1
    }
    
    println(s"done (time = ${(System.currentTimeMillis - before)/1000L} seconds).")
  }
}