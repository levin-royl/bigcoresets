package streaming.coresets

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.sampling.SampleTaker
import org.apache.spark.mllib.sampling.SamplerConfig
import org.apache.spark.mllib.sampling.StreamingTreeSampler
import org.apache.spark.mllib.sampling.TreeSampler
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import Args.Params
import Args.cli
import Domain._
import MySampleTaker.numNodesToSample
import univ.ml.BaseCoreset
import univ.ml.NonUniformCoreset
import univ.ml.SVDCoreset
import univ.ml.WeightedDoublePoint
import univ.ml.WeightedKMeansPlusPlusClusterer
import univ.ml.sparse.SparseWeightableVector
import univ.ml.sparse.algorithm.SparseCoresetAlgorithm
import univ.ml.sparse.SparseSVDCoreset
import univ.ml.sparse.algorithm.SparseNonUniformCoreset
import univ.ml.sparse.SparseWeightedKMeansPlusPlus
import java.util.concurrent.atomic.AtomicLong
import scala.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.Logging
import org.apache.spark.Logging

object MySampleTaker extends Serializable with Logging {
  val numNodesToSample = 2
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

object Timer {
  var startTime: Long = -1L

  var numThreads: Int = -1
  
  val timer: AtomicLong = new AtomicLong(0L)
}

class MySampleTaker(alg: BaseCoresetAlgorithm) extends SampleTaker[WPoint] with Logging {
  override def take(oelms: Iterable[WPoint], sampleSize: Int): Iterable[WPoint] = {
    val elms = oelms // .map(_.toWeightedDoublePoint)
    
    val res = if (elms.size > sampleSize) {
      val before = if (Timer.numThreads > 0) System.nanoTime else 0L
      
      val rr = alg.takeSample(elms)
      
      if (Timer.numThreads > 0) {
        Timer.timer.addAndGet(System.nanoTime - before)
      }
      
      require(rr.size <= sampleSize, s"requested sample size ${sampleSize} but got ${rr.size}")
      rr
    }
    else elms
    
    if (Timer.numThreads > 0 && Random.nextInt(10) == 0) {
      val s = 1000L*1000L*1000L
      val numCPUs = Timer.numThreads
      val totalTime = numCPUs*((System.nanoTime - Timer.startTime)/s)
      val samplingTime = Timer.timer.get/s
      val ratio = samplingTime.toDouble/totalTime.toDouble
      
      logWarning(s"${new Date} sampling total CPU[${numCPUs}] time is ${samplingTime}/${totalTime} seconds which is ${100.0*ratio}%")
    }

    res // .map(p => WPoint.create(p))
  }

  override def takeIds(elmsWithIds: Iterable[(Int, WPoint)], sampleSize: Int): Set[Int] = {
    val map = elmsWithIds.map(_.swap).toMap
    
    val sample = take(elmsWithIds.map(_._2), sampleSize)
    val res = sample.map(p => map.get(p).get).toSet
    
    res
  }
}

object App extends Serializable with Logging {
  def createCoresetAlg(params: Params): BaseCoresetAlgorithm = {
    val dense = params.denseData
    val alg = params.alg
    val algParams = params.algParams
    val sampleSize = params.sampleSize
    
    val a = alg.toLowerCase.trim 
    
    if (a.endsWith("svd")) {
      if (dense) {
        new BaseCoresetAlgorithm(
            denseAlg = Some(new SVDCoreset(algParams.toInt, sampleSize))
        )
      }
      else {
        new BaseCoresetAlgorithm(
            sparseAlg = Some(new SparseSVDCoreset(algParams.toInt, sampleSize))
        )
      }
    }
    else if (a.endsWith("kmeans")) {
      if (dense) {
        new BaseCoresetAlgorithm(
            denseAlg = Some(new NonUniformCoreset[WeightedDoublePoint](algParams.toInt, sampleSize))
        )
      }
      else {
        new BaseCoresetAlgorithm(
            sparseAlg = Some(new SparseNonUniformCoreset(algParams.toInt, sampleSize))
        )        
      }
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
    
    def sparseCoresetKmeans(data: Iterable[WPoint]): Array[Vector] = {
      val k = params.algParams.toInt
      val kmeansAlg = new SparseWeightedKMeansPlusPlus(k)
    
      val sample = data.map(_.toSparseWeightableVector).toList.asJava
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.sparse(
            c.getCenter.getVector.getDimension, 
            c.getCenter.getVector.iterator.asScala.map(ent => (ent.getIndex, ent.getValue)).toSeq
        )
      ).toArray
      
      centroids
    }
    
    if ("coreset-kmeans" == params.alg) {
      if (params.denseData) denseCoresetKmeans else sparseCoresetKmeans
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
    Timer.startTime = System.nanoTime
    
    val before = System.currentTimeMillis
    
    val params = cli(args)
    
    Timer.numThreads = if (params.generateProfile) {
      require(params.sparkParams.contains("spark.master"))
      
      params.sparkParams.getOrElse("spark.master", "local[-1]")
        .substring("local[".length).replace("]", "").toInt
    }
    else -1

    params.mode.toLowerCase.trim match {
      case "bulk" => testBulk(params)
      case "streaming" => testStreaming(params)
      case "evaluate" => evaluateCost(params)
      case _ => throw new RuntimeException(s"${params.mode} not supported")
    }
    
    logWarning(s"total runtime is ${(System.currentTimeMillis - before)/1000L} seconds")
  }
  
  private def getInputSource(args: Array[String]): String = {
    if (args.length > 0) args(0) else "localhost:9999"
  }
  
  private def getRootDir(args: Array[String]): String = {
    if (args.length > 1) args(1) else System.getProperty("user.home")
  }

/*
  private def repartition[T](rdd: RDD[T], oNumParts: Option[Int]): RDD[T] = {
    if (oNumParts.isDefined) rdd.repartition(oNumParts.get) else rdd
  }
  
  private def repartition[T](dstream: DStream[T], oNumParts: Option[Int]): DStream[T] = {
    if (oNumParts.isDefined) dstream.repartition(oNumParts.get) else dstream
  }
*/
  
  private def getLines(sc: SparkContext, params: Params): RDD[String] = {
    if (params.parallelism.isDefined) {
      sc.textFile(params.input, params.parallelism.get)
    }
    else {
      sc.textFile(params.input)
    }
  }
  
  private def getLines(ssc: StreamingContext, params: Params): DStream[String] = {
    val lines: DStream[String] = if (params.input.startsWith("socket://")) {
      val hostport = params.input.substring("socket://".length).split(':')
      require(hostport.length == 2)
      val hostname = hostport(0)
      val port = hostport(1).toInt
      ssc.socketTextStream(hostname, port)
    }
    else {
      ssc.textFileStream(params.input)
    }
    
    if (params.parallelism.isDefined) {
      lines.repartition(params.parallelism.get)
    } else {
      lines
    }
  }
  
  def evaluateCost(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir
//    val inputFile =  params.input
    
    val sparkConf = new SparkConf()
      .setAppName("EvaluateCost")
      .set("spark.ui.showConsoleProgress", "false")
    
    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    
    val sc = new SparkContext(sparkConf)
    val data = getLines(sc, params).map(parse).cache
    
    val calcCost = new CostCalc(sc)

    if (params.alg.endsWith("kmeans")) {
      calcCost.calcCost(data, params.output, CostCalc.kmeansCost)
    }
    else if (params.alg.endsWith("svd")) {
      calcCost.calcCost(data, params.output, CostCalc.svdCost)      
    }
    else {
      throw new RuntimeException(s"${params.alg} not supported")
    }
  }
  
  def testBulk(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir
//    val inputFile =  params.input
    
    val sparkConf = new SparkConf()
      .setAppName("BulkCoresets")
      .set("spark.ui.showConsoleProgress", "false")
//      .set("spark.storage.blockManagerSlaveTimeoutMs", "10000000")
//      .set("spark.executor.heartbeatInterval", "100000s")
//      .set("spark.akka.heartbeat.interval", "100000000s")
      
    params.sparkParams.foreach{ case(key, value) => sparkConf.set(key, value) }
    
    val sc = new SparkContext(sparkConf)
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    val data = getLines(sc, params).map(parse).cache
    
    val vecs: RDD[Array[Vector]] = if (params.alg.startsWith("coreset")) {
      val sampler = new TreeSampler[WPoint](
        SamplerConfig(numNodesToSample, params.sampleSize, params.localRDDs),
        new MySampleTaker(createCoresetAlg(params))
      )
      
      val parallelism = params.parallelism.getOrElse(100)
      val numPoints = data.count
      val optNumPointsPerPartition = params.sampleSize*numNodesToSample
      val optNumPartitions = numPoints/optNumPointsPerPartition
      val numPartitions = ((optNumPartitions/parallelism)*parallelism).toInt
      logWarning(s"repartitioning rdd from ${data.partitions.length} to ${numPartitions}")
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
    val lookBacktime = 3600L*1000L

    val sparkConf = new SparkConf()
      .setAppName("StreaimingCoresets")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.streaming.fileStream.minRememberDuration", lookBacktime.toString)
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
    
    val data = getLines(ssc, params).map(parse).cache

    val fileoutExt = getFileExt(params.output)
    val filename = params.output.substring(0, params.output.length - fileoutExt.length)
    
    val totalNumOfPoints = new AtomicLong(0L)
    val latestPointCount = new AtomicLong(0L)
    val latestSampleSize = new AtomicInteger(0)
    
    val resDStream: DStream[Array[Vector]] = if (params.alg.startsWith("coreset")) {
      val alg = createOnCoresetAlg(params)
      val samples = sampler.sample(
          data, 
          preCoreset = rdd => { 
            val cnt = rdd.count
            latestPointCount.set(cnt)
            totalNumOfPoints.addAndGet(cnt)
          },
          postCoreset = it => latestSampleSize.set(it.size)
      )
      
      val resStream = samples.transform(rdd => {
        val arr = rdd.collect
        
        val resRDD = if (!arr.isEmpty) {
          rdd.sparkContext.makeRDD(Seq(alg(arr)))
        } else rdd.sparkContext.emptyRDD[Array[Vector]]
        
        resRDD
      })
      
      resStream
    } else {
      val alg = createSparkAlg(params)
      data.transform(alg)
    }
    
    val computedResults = resDStream
      .map(mat => ComputedResult(
          mat, 
          System.currentTimeMillis,
          totalNumOfPoints.get,
          latestPointCount.get, 
          latestSampleSize.get,
          params.alg
      ))
      
    computedResults.filter(_.numPoints > 0).saveAsObjectFiles(filename, fileoutExt)
    
    // time to quit?
    val lastZeroTime = new AtomicLong(-1L)
    
    computedResults.foreachRDD(crRDD => {
      val cnt = crRDD.count

      if (cnt > 0) {
        assert(cnt == 1L)
        
        val single = crRDD.first
        val isZero = single.numPoints == 0
        val currTime = single.time
        
        logWarning(s"is zero = $isZero")
        
        if (!isZero) {
          lastZeroTime.set(-1L)
        }
        else if (lastZeroTime.get == -1L) {
          lastZeroTime.set(currTime)
        }
        else if (currTime - lastZeroTime.get > lookBacktime) {
          logWarning(s"${isZero} (${(currTime - lastZeroTime.get)/1000L} secs) Good Bye.")
  //        ssc.stop(true)
          System.exit(0)
        }
      }
    })
    
    
    logWarning("now!")
    Thread.sleep(10000L)

    ssc.start
    
    logWarning(s"[${new Date}] starting ...")

    ssc.awaitTermination
    
    logWarning(s"[${new Date}] done")
  }
  
  private def getFileExt(f: String): String = {
    val arr = f.split('.')
    if (arr.isEmpty) "" else arr.last
  }
}
