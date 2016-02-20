package streaming.coresets

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.sampling.SamplerConfig
import org.apache.spark.mllib.sampling.StreamingTreeSampler
import org.apache.spark.mllib.sampling.SampleTaker
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import univ.ml._
import Domain._
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.sampling.TreeSampler
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.distributed.RowMatrix

case class Params(
  verbose: Boolean = false,
  
  centerPathSuffix: String = "",
  
  denseData: Boolean = false,

  alg: String = "",
  
  batchSecs: Int = 10,
  
  parallelism: Int =  16,
  
  rootPath: String = System.getProperty("user.home"),

  input: String = "",
  
  mode: String = "",

  sparkParams: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired" -> "false"
//      "spark.kryoserializer.buffer.max.mb" -> "1024"
  )
)

object MySampleTaker extends Serializable {
  val sampleSize = 131072
  
  val numNodesToSample = 2
  
  val k = 100
  
  val dim = 500
  
  // https://github.com/C0rWin/Java-KMeans-Coreset
  val kmeansSamplingAlg: BaseCoreset[WeightedDoublePoint] = 
//    new KmeansCoreset(sampleSize)
//    new UniformCoreset[WeightedDoublePoint](sampleSize)
    new NonUniformCoreset[WeightedDoublePoint](k, sampleSize)
    
  val svdSamplingAlg: BaseCoreset[WeightedDoublePoint] = 
    new SVDCoreset(dim, sampleSize)

  def resorvoirSampling[T](
      elements: Iterable[T], 
      sampleSize: Int)(implicit m: ClassTag[T]): Iterable[T] = {
    val rand = new Random
    
    val resorvoir = new Array[T](sampleSize)
    var size = 0

    for (elm <- elements) {
      assert(elm != null)

      val i = size
      size += 1

      if (i >= sampleSize) {
        val j = rand.nextInt(size)

        if (j < sampleSize) {
          resorvoir(j) = elm
        }
      }
      else {
        resorvoir(i) = elm
      }
    }

    val res = if (size >= sampleSize) resorvoir else resorvoir.filter(_ != null)
    
    res
  }
}

import MySampleTaker._

case class WeightedDoublePointWithId(id: Int, coords: Array[Double]) 
  extends WeightedDoublePoint(coords, 1.0, "")

class MySampleTaker extends SampleTaker[WeightedDoublePoint] {
  override def take(elms: Iterable[WeightedDoublePoint], sampleSize: Int): Iterable[WeightedDoublePoint] = {
    val res = if (elms.size > sampleSize) {
//      println(s"sampling starting from ${elms.size} instances")
      
      val before = System.currentTimeMillis
      val alg = MySampleTaker.kmeansSamplingAlg
      
      val resSamples = if (alg != null) {
        alg.takeSample(elms.toList.asJava).asScala
      }
      else {
        MySampleTaker.resorvoirSampling(elms, sampleSize)
      }

//      println(s"sampling ${resSamples.size} out of ${elms.size} instances took :: ${(System.currentTimeMillis - before)}")
      resSamples
    }
    else elms

    res
  }

  override def takeIds(elmsWithIds: Iterable[(Int, WeightedDoublePoint)], sampleSize: Int): Set[Int] = {
    val withIds = elmsWithIds.map{ case(id, elm) => WeightedDoublePointWithId(id, elm.getPoint) }
    val sample = take(withIds, sampleSize)

    sample.map(_.asInstanceOf[WeightedDoublePointWithId].id).toSet
  }
}

object App extends Serializable {
  private def mergeMaps(l: Map[String, String], r: Map[String, String]) = {
    val res = new HashMap[String, String]
    l.foreach(res += _)
    r.foreach(t => if (!res.contains(t._1)) res += t)
    res.toMap
  }

  private def cli(args: Array[String]): Params = {
    val parser = new scopt.OptionParser[Params]("WCS Indexer") {
      head("Coreset tool", "1.0")
      
      opt[Unit]('v', "verbose") action {
        (_, c) => c.copy(verbose = true)
      } text ("verbose is a flag")
      
      opt[Unit]('d', "denseData") action {
        (_, c) => c.copy(denseData = true)
      } text ("vector data is dense rather than sparse")
      
      opt[String]("centerPathSuffix") required () action {
        (x, c) => c.copy(centerPathSuffix = x)
      } text("the suffix of the path to use for output centers RDD")
      
      opt[String]('a', "algorithm") required () action {
        (x, c) => c.copy(alg = x)
      } text ("supported algorithms are spark-kmeans, coreset-kmeans, svd")

      opt[Int]("batchSecs") optional () action {
        (x, c) => c.copy(batchSecs = x)
      } text("mini batch size in seconds (for streaming)")
      
      opt[Int]("parallelism") optional () action {
        (x, c) => c.copy(parallelism = x)
      } text("parallelism for effecting repartitioning")

      opt[String]('p', "rootPath") optional () action {
        (x, c) => c.copy(rootPath = x)
      } text("the root path directory can be on FS or HDFS")

      opt[String]('i', "input") required () action {
        (x, c) => c.copy(input = x)
      } text("input file or source")
      
      opt[String]('m', "mode") required () action {
        (x, c) => c.copy(mode = x)
      } text("mode can be 'bulk' or 'streaming'")
      
      opt[Map[String, String]]("sparkParams") valueName ("k1=v1, k2=v2, ...") action {
        (x, c) => c.copy(sparkParams = mergeMaps(x, c.sparkParams))
      } text ("these are parameters to pass on for the spark configuration")

      help("help") text ("for help contact royl@il.ibm.com")
    }

    val ores = parser.parse(args.toSeq, Params())
    
    if (ores.isEmpty) {
      throw new RuntimeException("bad input paramaters")
    }
    
    ores.get
  }
  
  def main(args: Array[String]) {
    val before = System.currentTimeMillis
    
    val params = cli(args)

    params.mode.toLowerCase.trim match {
      case "bulk" => testBulk(params)
      case "streaming" => testStreaming(params)
      case _ => throw new RuntimeException(s"${params.mode} not supported")
    }
    
    println(s"total runtime is ${(System.currentTimeMillis - before)/1000L} seconds")
  }
  
  private def getInputSource(args: Array[String]): String = {
    if (args.length > 0) args(0) else "localhost:9999"
  }
  
  private def getRootDir(args: Array[String]): String = {
    if (args.length > 1) args(1) else System.getProperty("user.home")
  }
  
  def testBulk(params: Params): Unit = {
    val rootDir = params.rootPath
    val sparkCheckpointDir = s"${rootDir}/spark-temp/checkpoint"
    val sparkLocalDir = s"${rootDir}/spark-temp/local"
    val inputFile =  params.input
    
    val sparkConf = new SparkConf()
      .setAppName("BulkCoresets")
      .set("spark.local.dir", sparkLocalDir)
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.storage.blockManagerSlaveTimeoutMs", "10000000")
      .set("spark.executor.heartbeatInterval", "100000s")
      .set("spark.akka.heartbeat.interval", "100000000s")
      
    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }
    
    def parse = if (params.denseData) parseDense _ else parseSparse _

    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(inputFile).map(parse)
    
    if ("spark-kmeans" == params.alg) {
      val points = data.map(p => Vectors.dense(p.getPoint)).cache
      val model = KMeans.train(points, k, Int.MaxValue)
      
      val centers = model.clusterCenters
      val centersRDD = sc.makeRDD(Seq(centers))
      
      centersRDD.saveAsObjectFile(s"${sparkLocalDir}/${params.centerPathSuffix}/center.vecs")
    }
    else if ("coreset-kmeans" == params.alg) {
      val sampler = new TreeSampler[WPoint](
        SamplerConfig(numNodesToSample, sampleSize, false),
        new MySampleTaker
      )
      
      val parallelism = params.parallelism
      val numPoints = data.count
      val optNumPointsPerPartition = sampleSize*numNodesToSample
      val optNumPartitions = numPoints/optNumPointsPerPartition
      val numPartitions = ((optNumPartitions/parallelism)*parallelism).toInt
      println(s"repartitioning rdd from ${data.partitions.length} to ${numPartitions}")
      val samples = sampler.sample(data.repartition(numPartitions).cache)

      val kmeansAlg = new WeightedKMeansPlusPlusClusterer[WPoint](k)
      val sample = samples.toList.asJava
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.dense(c.getCenter.getPoint)
      ).toArray

      val centersRDD = sc.makeRDD(Seq(centroids))
      centersRDD.saveAsObjectFile(s"${sparkLocalDir}/${params.centerPathSuffix}/center.vecs")
    }
    else if ("spark-svd" == params.alg) {
      val rows = data.map(p => Vectors.dense(p.getPoint)).cache
      val mat = new RowMatrix(rows)
      
      val model = mat.computeSVD(dim, computeU = true)
    }
    else if ("coreset-svd" == params.alg) {
    }
    else {
      throw new RuntimeException(s"unsupported algorithm {$params.alg}")
    }
  }
  
  def testStreaming(params: Params): Unit = {
    val rootDir = params.rootPath
    val sparkCheckpointDir = s"${rootDir}/spark-temp/checkpoint"
    val sparkLocalDir = s"${rootDir}/spark-temp/local"
    val hostport = params.input.split(':')
    require(hostport.length == 2)
    val hostname = hostport(0)
    val port = hostport(1).toInt

    val sparkConf = new SparkConf()
      .setAppName("StreaimingCoresets")
      .set("spark.local.dir", sparkLocalDir)
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.streaming.receiver.maxRate", (1024*32).toString)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

//    sparkConf.registerKryoClasses(kryoClasses)

    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }

    val ssc = new StreamingContext(sparkConf, Seconds(params.batchSecs))
    ssc.checkpoint(sparkCheckpointDir)

    val sampler = new StreamingTreeSampler[WPoint](
        SamplerConfig(numNodesToSample, sampleSize, true),
        new MySampleTaker,
        params.batchSecs
    )
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    val data = ssc.socketTextStream(hostname, port).map(parse)

    if ("coreset-kmeans" == params.alg) {
      val samples = sampler.sample(data)
      val centroids = coresetKmeansCentroids(samples, params.batchSecs)
      centroids.saveAsObjectFiles(s"${sparkLocalDir}/${params.centerPathSuffix}/center", "vecs")
    }
    else if ("spark-kmeans" == params.alg) {
      val centroids = getCentersDStream(data, params.batchSecs)
      centroids.saveAsObjectFiles(s"${sparkLocalDir}/${params.centerPathSuffix}/center", "vecs")
    }
    else if ("spark-svd" == params.alg) {
      throw new RuntimeException(s"spark-svd does not exist for streaming")
    }
    else if ("coreset-svd" == params.alg) {
      sampler.sample(data, it => {
        // TODO: write code here
        val svdAlg = new WeightedKMeansPlusPlusClusterer[WPoint](k)
        svdAlg.cluster(it.toList.asJava)
      })
    }
    else {
      throw new RuntimeException(s"unsupported algorithm {$params.alg}")
    }
    
    ssc.start
//    ssc.awaitTermination(1000L*60L*15L)
    ssc.awaitTermination
  }
  
  def getCentersDStream(data: DStream[WPoint], batchSecs: Long): DStream[Array[Vector]] = {
    val kmeansAlg = new StreamingKMeans()
      .setK(k)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    var model = kmeansAlg.latestModel

    data.transform(rdd => {
      val before = System.currentTimeMillis
      val numPoints = rdd.count
      val points = rdd.map(p => Vectors.dense(p.getPoint))
      
      model = model.update(points, kmeansAlg.decayFactor, kmeansAlg.timeUnit)
      val centers = model.clusterCenters

      val sc = rdd.sparkContext
      val centersRDD = sc.makeRDD(Seq(centers))
      
      lazy val deltaT = System.currentTimeMillis - before
      println(s"stream processing for $numPoints points duration is $deltaT ms")
      require(deltaT < 1000L*batchSecs, s"$deltaT")
      
      centersRDD
    })
  }
  
  private def coresetKmeansCentroids(points: DStream[WPoint], batchSecs: Long): DStream[Array[Vector]] = {
    points.transform(rdd => {
      val before = System.currentTimeMillis
      
      val numPoints = rdd.count
      val kmeansAlg = new WeightedKMeansPlusPlusClusterer[WPoint](k)
      val sample = rdd.collect.toList.asJava
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.dense(c.getCenter.getPoint)
      ).toArray
      
      val sc = rdd.sparkContext
      val centroidsRDD = sc.makeRDD(Seq(centroids))

/*      
      val costFunc = new WSSSE()
      val cost = costFunc.cost(centroids)

      rdd.zipWithIndex.filter(_._2 == 0L).map(_ => cost)
*/
      
      lazy val deltaT = System.currentTimeMillis - before
      println(s"stream processing for $numPoints points duration is $deltaT ms")
      require(deltaT < 1000L*batchSecs, s"$deltaT")
      
      centroidsRDD
    })
  }
}
