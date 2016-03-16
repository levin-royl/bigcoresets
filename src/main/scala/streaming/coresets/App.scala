package streaming.coresets

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.util.LinearAlgebraUtil._
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
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.SparseMatrix
import org.apache.spark.mllib.linalg.DenseMatrix
import univ.ml.sparse.SparseSVDCoreset
import univ.ml.sparse.SparseWeightableVector
import univ.ml.sparse.algorithm.SparseCoresetAlgorithm

import org.apache.spark.rdd.RDD

case class Params(
  verbose: Boolean = false,
  
  denseData: Boolean = false,

  localRDDs: Boolean = false,

  alg: String = "",

  algParams: String = "",
  
  sampleSize: Int = -1,
  
  batchSecs: Int = 10,
  
  parallelism: Int = -1,

  input: String = "",
  
  output: String = "",
  
  checkpointDir: String = "",
  
  mode: String = "",

  sparkParams: Map[String, String] = Map(
//    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
//    "spark.kryo.registrationRequired" -> "false"
//    "spark.kryoserializer.buffer.max.mb" -> "1024"
  )
)

object MySampleTaker extends Serializable {
  val numNodesToSample = 2
}

import MySampleTaker._

class WPointWithId(val id: Int, inner: WPoint) extends WPoint {
  override def isSparse: Boolean = inner.isSparse
  
  override def toWeightedDoublePoint: WeightedDoublePoint = inner.toWeightedDoublePoint
  
  override def toSparseWeightableVector: SparseWeightableVector = inner.toSparseWeightableVector
  
  override def toVector: Vector = inner.toVector
}

class BaseCoresetAlgorithm(
    denseAlg: Option[BaseCoreset[WeightedDoublePoint]] = None,
    sparseAlg: Option[SparseCoresetAlgorithm] = None) extends Serializable {
  
  require(denseAlg.isDefined != sparseAlg.isDefined)
  
  def denseTakeSample(elms: Iterable[WeightedDoublePoint]): Iterable[WPoint] = {
    require(denseAlg.isDefined)
    val lst = elms.toList.asJava
    val res = denseAlg.get.takeSample(lst)
    res.asScala.map(p => WPoint(p))
  }
  
  def sparseTakeSample(elms: Iterable[SparseWeightableVector]): Iterable[WPoint] = {
    require(sparseAlg.isDefined)
    val lst = elms.toList.asJava
    val res = sparseAlg.get.takeSample(lst)
    res.asScala.map(p => WPoint(p))
  }
  
  def takeSample(elms: Iterable[WPoint]): Iterable[WPoint] = {
    if (denseAlg.isDefined) {
      denseTakeSample(elms.map(_.toWeightedDoublePoint))
    }
    else {
      require(sparseAlg.isDefined)
      sparseTakeSample(elms.map(_.toSparseWeightableVector))
    }
  }
}

class MySampleTaker(alg: BaseCoresetAlgorithm) extends SampleTaker[WPoint] {
  override def take(oelms: Iterable[WPoint], sampleSize: Int): Iterable[WPoint] = {
    val elms = oelms // .map(_.toWeightedDoublePoint)
    
    val res = if (elms.size > sampleSize) {
      val rr = alg.takeSample(elms)
      require(rr.size <= sampleSize, s"requested sample size ${sampleSize} but got ${rr.size}")
      rr
    }
    else elms

    res // .map(p => WPoint.create(p))
  }

  override def takeIds(elmsWithIds: Iterable[(Int, WPoint)], sampleSize: Int): Set[Int] = {
    val withIds = elmsWithIds.map{ case(id, elm) => new WPointWithId(id, elm) }
    val sample = take(withIds, sampleSize)

    sample.map(_.asInstanceOf[WPointWithId].id).toSet
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
      
      opt[Unit]('l', "localRDDs") action {
        (_, c) => c.copy(localRDDs = true)
      } text ("do all sampling locally in driver")
      
      opt[String]('a', "algorithm") required () action {
        (x, c) => c.copy(alg = x)
      } text ("supported algorithms are spark-kmeans, coreset-kmeans, coreset-svd")
      
      opt[String]("algorithmParams") required () action {
        (x, c) => c.copy(algParams = x)
      } text ("send paramaters to algorithm")
  
      opt[Int]("sampleSize") required () action {
        (x, c) => c.copy(sampleSize = x)
      } text("sample size for coresets")
      
      opt[Int]("batchSecs") required () action {
        (x, c) => c.copy(batchSecs = x)
      } text("mini batch size in seconds (for streaming)")
      
      opt[Int]("parallelism") required () action {
        (x, c) => c.copy(parallelism = x)
      } text("parallelism for effecting repartitioning")

      opt[String]('c', "checkpointDir") optional () action {
        (x, c) => c.copy(checkpointDir = x)
      } text("the checkpointDir for spark, can be on FS or HDFS")

      opt[String]('i', "input") required () action {
        (x, c) => c.copy(input = x)
      } text("input file or source")
      
      opt[String]('o', "output") required () action {
        (x, c) => c.copy(output = x)
      } text("the suffix of the path to use for output centers RDD")
      
      opt[String]('m', "mode") required () action {
        (x, c) => c.copy(mode = x)
      } text("mode can be 'bulk', 'streaming' or 'evaluate'")
      
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
  
  // TODO: fix this method
  def createCoresetAlg(params: Params): BaseCoresetAlgorithm = {
    val dense = params.denseData
    val alg = params.alg
    val algParams = params.algParams
    val sampleSize = params.sampleSize
    
    val a = alg.toLowerCase.trim 
    
    if (a.endsWith("svd") && dense) {
      new BaseCoresetAlgorithm(
          denseAlg = Some(new SVDCoreset(algParams.toInt, sampleSize))
      )
    }
//    else if (a.endsWith("svd") && !dense) {
//      new SparseSVDCoreset(algParams.toInt, sampleSize)
//    }
    else if (a.endsWith("kmeans")) {
      new BaseCoresetAlgorithm(
          denseAlg = Some(new NonUniformCoreset[WeightedDoublePoint](algParams.toInt, sampleSize))
      )
    }
    else { 
      throw new RuntimeException(s"unknown algorithm ${alg}")
    }
  }
  
  def createOnCoresetAlg(params: Params): (Iterable[WPoint] => Array[Vector]) = {
    def denseCoresetKmeans(data: Iterable[WPoint]): Array[Vector] = {
      val k = params.algParams.toInt
      val kmeansAlg = new WeightedKMeansPlusPlusClusterer[WeightedDoublePoint](k)
    
      val sample = data.map(_.toWeightedDoublePoint).toList.asJava
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.dense(c.getCenter.getPoint)
      ).toArray
      
      centroids
    }
    
    if ("coreset-kmeans" == params.alg && params.denseData) {
      denseCoresetKmeans
    }
    else {
      throw new UnsupportedOperationException(s"${params.alg} in mode dense = ${params.denseData}")
    }
  }
  
  def createSparkAlg(params: Params): (RDD[WPoint] => RDD[Array[Vector]]) = {    
    def sparkStreaingKMeans(data: RDD[WPoint]): RDD[Array[Vector]] = {
      val k = params.algParams.toInt
      val kmeansAlg = new StreamingKMeans()
        .setK(k)
        .setDecayFactor(1.0)
        .setRandomCenters(2, 0.0)
  
      var model = kmeansAlg.latestModel
  
      val points = data.map(_.toVector)
      
      model = model.update(points, kmeansAlg.decayFactor, kmeansAlg.timeUnit)
      val centers = model.clusterCenters

      data.sparkContext.makeRDD(Seq(centers))
    }
    
    def sparkBulkKmeans(data: RDD[WPoint]): RDD[Array[Vector]] = {
      val points = data.map(_.toVector).cache
      val model = KMeans.train(points, params.algParams.toInt, Int.MaxValue)
      
      val centers = model.clusterCenters
      data.sparkContext.makeRDD(Seq(centers))
    }

    def denseBulkSparkSVD(data: RDD[WPoint]): RDD[Array[Vector]] = {
      val rows = data.map(_.toVector).cache
      val mat = new RowMatrix(rows)

      val dim = params.algParams.toInt
      val model = mat.computeSVD(dim, computeU = false)

      val V = model.V
      val Vents = (0 until V.numRows)
        .map(i => Vectors.dense((0 until V.numCols).map(j => V(i, j)).toArray)).toArray
      
      data.sparkContext.makeRDD(Seq(Vents))
    }
    
    if ("spark-kmeans" == params.alg && "bulk" == params.mode) {
      sparkBulkKmeans
    }
    else if ("spark-kmeans" == params.alg && "streaming" == params.mode) {
      sparkStreaingKMeans
    }
    else if ("spark-svd" == params.alg  && "bulk" == params.mode && params.denseData) {
      denseBulkSparkSVD
    }
    else {
      throw new UnsupportedOperationException(s"${params.alg} in mode dense = ${params.denseData}")
    }
  }
  
  def main(args: Array[String]) {    
    val before = System.currentTimeMillis
    
    val params = cli(args)

    params.mode.toLowerCase.trim match {
      case "bulk" => testBulk(params)
      case "streaming" => testStreaming(params)
      case "evaluate" => evaluateCost(params)
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
  
  def evaluateCost(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir
    val inputFile =  params.input
    
    val sparkConf = new SparkConf()
      .setAppName("EvaluateCost")
      .set("spark.ui.showConsoleProgress", "false")
    
    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(inputFile).repartition(params.parallelism).map(parse).cache
    
    val calcCost = new CostCalc(sc)

    if (params.alg.endsWith("kmeans")) {
      calcCost.calcCost(data, params.input, CostCalc.kmeansCost)
    }
    else if (params.alg.endsWith("svd")) {
      calcCost.calcCost(data, params.input, CostCalc.svdCost)      
    }
    else {
      throw new RuntimeException(s"${params.alg} not supported")
    }
  }
  
  def testBulk(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir
    val inputFile =  params.input
    
    val sparkConf = new SparkConf()
      .setAppName("BulkCoresets")
      .set("spark.ui.showConsoleProgress", "false")
//      .set("spark.storage.blockManagerSlaveTimeoutMs", "10000000")
//      .set("spark.executor.heartbeatInterval", "100000s")
//      .set("spark.akka.heartbeat.interval", "100000000s")
      
    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }
    
    val sc = new SparkContext(sparkConf)
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    val data = sc.textFile(inputFile).repartition(params.parallelism).map(parse).cache
    
    val vecs: RDD[Array[Vector]] = if (params.alg.startsWith("coreset")) {
      val sampler = new TreeSampler[WPoint](
        SamplerConfig(numNodesToSample, params.sampleSize, params.localRDDs),
        new MySampleTaker(createCoresetAlg(params))
      )
      
      val parallelism = params.parallelism
      val numPoints = data.count
      val optNumPointsPerPartition = params.sampleSize*numNodesToSample
      val optNumPartitions = numPoints/optNumPointsPerPartition
      val numPartitions = ((optNumPartitions/parallelism)*parallelism).toInt
      println(s"repartitioning rdd from ${data.partitions.length} to ${numPartitions}")
      val samples = sampler.sample(data)
      
      val alg = createOnCoresetAlg(params)
      sc.makeRDD(Seq(alg(samples)))
    }
    else {
      val alg = createSparkAlg(params)
      alg(data)
    }
    
    vecs.saveAsObjectFile(s"${params.output}")
  }
  
  def testStreaming(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir

    val sparkConf = new SparkConf()
      .setAppName("StreaimingCoresets")
      .set("spark.ui.showConsoleProgress", "false")
//      .set("spark.streaming.receiver.maxRate", (1024*32).toString)
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//      .set("spark.streaming.backpressure.enabled", "true")
//      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }

    val ssc = new StreamingContext(sparkConf, Seconds(params.batchSecs))
    ssc.checkpoint(sparkCheckpointDir)

    val sampler = new StreamingTreeSampler[WPoint](
        SamplerConfig(numNodesToSample, params.sampleSize, params.localRDDs),
        new MySampleTaker(createCoresetAlg(params)),
        params.batchSecs
    )

    def parse = if (params.denseData) parseDense _ else parseSparse _

    val lines: DStream[String] = if (params.input.startsWith("socket://")) {
      val hostport = params.input.substring("socket://".length).split(':')
      require(hostport.length == 2)
      val hostname = hostport(0)
      val port = hostport(1).toInt
      ssc.socketTextStream(hostname, port)
    }
    else {
      assert(params.input.startsWith("hdfs://") || params.input.startsWith("file://"))
      ssc.textFileStream(params.input)
    }

    val data = lines.repartition(params.parallelism).map(parse).cache

    val fileoutExt = getFileExt(params.output)
    val filename = params.output.substring(0, params.output.length - fileoutExt.length)
    
    val resDStream: DStream[Array[Vector]] = if (params.alg.startsWith("coreset")) {
      val alg = createOnCoresetAlg(params)
      val samples = sampler.sample(data)
      
      samples.transform(rdd => {
        rdd.sparkContext.makeRDD(Seq(alg(rdd.collect)))
      })
    } else {
      val alg = createSparkAlg(params)
      data.transform(alg)
    }
    
    resDStream.saveAsObjectFiles(filename, fileoutExt)
    
    ssc.start
//    ssc.awaitTermination(1000L*60L*15L)
    ssc.awaitTermination
  }
  
  private def getFileExt(f: String): String = {
    val arr = f.split('.')
    if (arr.isEmpty) "" else arr.last
  }
}
