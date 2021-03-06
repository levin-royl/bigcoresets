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
import GenUtil._
import MySampleTaker.numNodesToSample
import univ.ml.BaseCoreset
import univ.ml.NonUniformCoreset
import univ.ml.SVDCoreset
import univ.ml.WeightedDoublePoint
import univ.ml.WeightedKMeansPlusPlusClusterer
import univ.ml.sparse.SparseWeightableVector
import univ.ml.sparse.algorithm._
import univ.ml.sparse.SparseSVDCoreset
import java.util.concurrent.atomic.AtomicLong

import scala.util.Random
import java.util.concurrent.ConcurrentHashMap
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.Logging
import org.apache.spark.Logging
import org.apache.spark.mllib.sampling.RDDLike
import org.apache.spark.mllib.sampling.RDDLikeWrapper
import org.apache.spark.mllib.sampling.RDDLikeIterable
import org.apache.spark.mllib.sampling.GenUtil._
import org.apache.spark.streaming.Milliseconds
import univ.ml.UniformCoreset

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

/*
object Timer {
  var startTime: Long = -1L

  var numThreads: Int = -1
  
  val timer: AtomicLong = new AtomicLong(0L)
}
*/

class MySampleTaker(alg: BaseCoresetAlgorithm) extends SampleTaker[WPoint] with Logging {
  override def take(oelms: Iterable[WPoint], sampleSize: Int): Iterable[WPoint] = {
    val before = System.currentTimeMillis
    
    val elms = oelms // .map(_.toWeightedDoublePoint)
    
    val res = if (elms.size > sampleSize) {
//      val before = if (Timer.numThreads > 0) System.nanoTime else 0L
      
      val rr = alg.takeSample(elms)

      // TODO: debug
//      println(s"pre-sample size = ${elms.size} and post-sample size = ${rr.size}")
      
//      if (Timer.numThreads > 0) {
//        Timer.timer.addAndGet(System.nanoTime - before)
//      }
      
      require(rr.size <= sampleSize, s"requested sample size ${sampleSize} but got ${rr.size}")
      rr
    }
    else elms

/*    
    if (Timer.numThreads > 0 && Random.nextInt(10) == 0) {
      val s = 1000L*1000L*1000L
      val numCPUs = Timer.numThreads
      val totalTime = numCPUs*((System.nanoTime - Timer.startTime)/s)
      val samplingTime = Timer.timer.get/s
      val ratio = samplingTime.toDouble/totalTime.toDouble
      
      mylog(s"${new Date} sampling total CPU[${numCPUs}] time is ${samplingTime}/${totalTime} seconds which is ${100.0*ratio}%")
    }
*/

    println(s"ran sampling on node: ${getHostName} (took ${(System.currentTimeMillis - before)} ms)")
    res // .map(p => WPoint.create(p))
  }

  override def takeIds(elmsWithIds: Iterable[(Int, WPoint)], sampleSize: Int): Iterable[Int] = {
    val map = elmsWithIds.map(_.swap).toMap
    
    val sample = take(elmsWithIds.map(_._2), sampleSize)
    
    sample.map(p => map.get(p).get)
  }
}

object App extends Serializable with Logging {
  def createCoresetAlg(params: Params): BaseCoresetAlgorithm = {
    val dense = params.denseData
    val alg = params.alg
    val algParams = params.algParams
    val sampleSize = params.sampleSize
    
    val a = alg.toLowerCase.trim 
    
    val coresetAlg = if (a.endsWith("svd")) {
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
        if (!a.contains("uniform")) {
          new BaseCoresetAlgorithm(
              denseAlg = Some(new NonUniformCoreset[WeightedDoublePoint](algParams.toInt, sampleSize))
          )
        }
        else {
          new BaseCoresetAlgorithm(
              denseAlg = Some(new UniformCoreset[WeightedDoublePoint](sampleSize))
          )
        }
      }
      else {
        if (!a.contains("uniform")) {
          if (a.contains("coreset2")) {
            new BaseCoresetAlgorithm(
                sparseAlg = Some(new SparseKmeansCoresetAlgorithm(sampleSize))
            )
          }
          else new BaseCoresetAlgorithm(
              sparseAlg = Some(new SparseNonUniformCoreset(new KMeansPlusPlusSeed(algParams.toInt), sampleSize))
          )
        }
        else {
          new BaseCoresetAlgorithm(
              sparseAlg = Some(new SparseUniformCoreset(sampleSize))
          )          
        }
      }
    }
    else { 
      throw new RuntimeException(s"unknown algorithm ${alg}")
    }
    
    println(s"createCoresetAlg created instance of: ${coresetAlg.getClass}")
    
    coresetAlg
  }

  // this method return a 'method' that applies the in-memory algorithm (kmeans, SVD, etc.)
  def createOnCoresetAlg(params: Params): (Iterable[WPoint] => Array[Vector]) = {
    def denseCoresetKmeans(data: Iterable[WPoint]): Array[Vector] = {
      mylog(s"running denseCoresetKmeans on ${data.size} points")
      
      val k = params.algParams.toInt
      val kmeansAlg = new WeightedKMeansPlusPlusClusterer[WeightedDoublePoint](k)
    
      val sample = data.map(_.toWeightedDoublePoint).toList.asJava
      
      mylog(s"got sample of size ${sample.size}, now applying in-mem algorithm ${kmeansAlg.getClass}")
      
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.dense(c.getCenter.getPoint)
      ).toArray
      
      mylog(s"computed ${centroids.size} centroids")

      centroids
    }
    
    def sparseCoresetKmeans(data: Iterable[WPoint]): Array[Vector] = {
      mylog(s"running sparseCoresetKmeans on ${data.size} points")
      
      val k = params.algParams.toInt
      val kmeansAlg = new SparseWeightedKMeansPlusPlus(k)
    
      val sample = data.map(_.toSparseWeightableVector).toList.asJava
      
      mylog(s"got sample of size ${sample.size}, now applying in-mem algorithm ${kmeansAlg.getClass}")
      
      val centroids = kmeansAlg.cluster(sample).asScala.map(c => 
        Vectors.sparse(
            c.getCenter.getVector.getDimension, 
            c.getCenter.getVector.iterator.asScala.map(ent => (ent.getIndex, ent.getValue)).toSeq
        )
      ).toArray
      
      mylog(s"computed ${centroids.size} centroids")
      
      centroids
    }
    
    def sparseCoresetSVD(data: Iterable[WPoint]): Array[Vector] = {
      mylog(s"running sparseCoresetSVD on ${data.size} points")
      
      val k = params.algParams.toInt
      // TODO: Artem --- replace with SparseWeightedSVD here
      val svdAlg = new SparseWeightedKMeansPlusPlus(k)
    
      val sample = data.map(_.toSparseWeightableVector).toList.asJava
      
      mylog(s"got sample of size ${sample.size}, now applying in-mem algorithm ${svdAlg.getClass}")
      
      val axis = svdAlg.cluster(sample).asScala.map(c => 
        Vectors.sparse(
            c.getCenter.getVector.getDimension, 
            c.getCenter.getVector.iterator.asScala.map(ent => (ent.getIndex, ent.getValue)).toSeq
        )
      ).toArray
      
      mylog(s"computed ${axis.size} axis")
      
      axis
    }
    
    if ("coreset-kmeans" == params.alg || "coreset2-kmeans" == params.alg || "coreset-uniform-kmeans" == params.alg) {
      if (params.denseData) denseCoresetKmeans else sparseCoresetKmeans
    }
    else if (("coreset-svd" == params.alg || "coreset-uniform-svd" == params.alg)&& !params.denseData) {
      sparseCoresetSVD
    }
    else {
      throw new UnsupportedOperationException(s"${params.alg} in mode dense = ${params.denseData}")
    }
  }
  
  def createSparkAlg(params: Params): (RDD[WPoint] => RDD[ComputedResult]) = { 
    val algName = params.alg
    
    def sparkStreaingKMeans(data: RDD[WPoint]): RDD[ComputedResult] = {
      val dim = params.dim
      val k = params.algParams.toInt
      val kmeansAlg = new StreamingKMeans()
        .setK(k)
        .setDecayFactor(1.0)
        .setRandomCenters(dim, 0.0)
  
      var model = kmeansAlg.latestModel
  
      val points = data.map(_.toVector)
      
      model = model.update(points, kmeansAlg.decayFactor, kmeansAlg.timeUnit)
      val centers = model.clusterCenters
      val cnt = data.count
      val res = ComputedResult(centers, cnt, cnt.toInt, algName)

      data.sparkContext.makeRDD(Seq(res))
    }
    
    def sparkBulkKmeans(data: RDD[WPoint]): RDD[ComputedResult] = {
      val points = data.map(_.toVector).cache
      val model = KMeans.train(points, params.algParams.toInt, Int.MaxValue)
      
      val centers = model.clusterCenters
      val cnt = data.count
      val res = ComputedResult(centers, cnt, cnt.toInt, algName)
      
      data.sparkContext.makeRDD(Seq(res))
    }

    def denseBulkSparkSVD(data: RDD[WPoint]): RDD[ComputedResult] = {
      val rows = data.map(_.toVector).cache
      val mat = new RowMatrix(rows)

      val dim = params.algParams.toInt
      val model = mat.computeSVD(dim, computeU = false)

      val V = model.V
      val Vents = (0 until V.numRows)
        .map(i => Vectors.dense((0 until V.numCols).map(j => V(i, j)).toArray)).toArray
      
      val cnt = data.count
      val res = ComputedResult(Vents, cnt, cnt.toInt, algName)
      
      data.sparkContext.makeRDD(Seq(res))
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
    mylog("starting up ...")
    
//    Timer.startTime = System.nanoTime
    
    val before = System.currentTimeMillis
    
    val params = cli(args)
    
/*    
    Timer.numThreads = if (params.generateProfile) {
      require(params.sparkParams.contains("spark.master"))
      
      params.sparkParams.getOrElse("spark.master", "local[-1]")
        .substring("local[".length).replace("]", "").toInt
    }
    else -1
*/
    
    params.mode.toLowerCase.trim match {
      case "bulk" => testBulk(params)
      case "streaming" => testStreaming(params)
      case "evaluate" => evaluateCost(params)
      case _ => throw new RuntimeException(s"${params.mode} not supported")
    }
    
    mylog(s"total runtime is ${(System.currentTimeMillis - before)/1000L} seconds")
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
  
  private def getLines(ssc: StreamingContext, params: Params, msg: String = null): DStream[String] = {
    val lines$: DStream[String] = if (params.input.startsWith("socket://")) {
      val hostport = params.input.substring("socket://".length).split(':')
      require(hostport.length == 2)
      val hostname = hostport(0)
      val port = hostport(1).toInt
      ssc.socketTextStream(hostname, port)
    }
    else {
      ssc.textFileStream(params.input)
    }
    
    val lines = if (msg != null) lines$.map(reportAndGet(msg)) else lines$
    
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
    sc.setCheckpointDir(sparkCheckpointDir)
    
    val data = getLines(sc, params).map(parse)

    println(s"processing data points on ${data.partitions.length} partitions")
    
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
    sc.setCheckpointDir(sparkCheckpointDir)
    
    def parse = if (params.denseData) parseDense _ else parseSparse _
    val data = getLines(sc, params).map(parse).cache
    
    val vecs: RDD[ComputedResult] = if (params.alg.startsWith("coreset")) {
      val sampler = new TreeSampler[WPoint](
        SamplerConfig(numNodesToSample, params.sampleSize, params.localRDDs),
        new MySampleTaker(createCoresetAlg(params))
      )
      
      val parallelism = params.parallelism.getOrElse(100)
      val numPoints = data.count
      val optNumPointsPerPartition = params.sampleSize*numNodesToSample
      val optNumPartitions = numPoints/optNumPointsPerPartition
      val numPartitions = ((optNumPartitions/parallelism)*parallelism).toInt
      mylog(s"repartitioning rdd from ${data.partitions.length} to ${numPartitions}")
      
      val alg = createOnCoresetAlg(params)
      def processSample = makeProcessSample(alg, params.alg)
      val algRes = sampler.sample(data)
      val rddLikeRes = processSample(data, new RDDLikeIterable(algRes))
      
      RDDLike.toRDD(rddLikeRes, data.sparkContext)
    }
    else {
      val alg = createSparkAlg(params)
      alg(data)
    }
    
    vecs.saveAsObjectFile(s"${params.output}")
  }
  
  private def makeProcessSample(
      alg: Iterable[WPoint] => Array[Vector],
      algName: String): (RDD[WPoint], RDDLike[WPoint]) => RDDLike[ComputedResult] = {
    
    (rdd: RDD[WPoint], sample: RDDLike[WPoint]) => {
      val dataSize = rdd.count
      val localSample = sample.collect

      mylog(s"data size = ${dataSize} final sample size = ${localSample.size}")

      val mat = if (dataSize > 0) {
        alg(localSample)
      }
      else {
        Array.empty[Vector]
      }

      new RDDLikeIterable(Seq(
          ComputedResult(mat, dataSize, localSample.size, algName))
      )
    }
  }

  private def reportAndGet[T](msg: String)(elm: T) = {
    mylog(msg);
    elm
  }
  
  def testStreaming(params: Params): Unit = {
    val sparkCheckpointDir = params.checkpointDir
    val lookBacktime = 100L*params.batchSecs.toLong*1000L

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
    ssc.remember(Milliseconds(lookBacktime))
    
    mylog(s"set time-to-remember to ${lookBacktime/1000L} seconds")
    
    val sampler = new StreamingTreeSampler[WPoint](
        SamplerConfig(numNodesToSample, params.sampleSize, params.localRDDs),
        new MySampleTaker(createCoresetAlg(params)),
        params.batchSecs
    )

    def parse = if (params.denseData) parseDense _ else parseSparse _
    
    val data = getLines(ssc, params, "stream reading begun ...")
      .map(reportAndGet(s"now reading ..."))
      .map(parse)
      .cache

    val fileoutExt = getFileExt(params.output)
    val filename = params.output.substring(0, params.output.length - fileoutExt.length)
    
    val computedResults: DStream[ComputedResult] = {
      val dres = if (params.alg.startsWith("coreset")) {
        val alg = createOnCoresetAlg(params)
        def processSample = makeProcessSample(alg, params.alg)
        
        val r = sampler.sample(data, processSample)
        
        r
      } 
      else {
        val alg = createSparkAlg(params)
        
        data.transform(alg)
      }
      
      dres
    }
    
    computedResults.map(reportAndGet("almost done ...")).filter(_.numPoints > 0).repartition(1)
      .saveAsObjectFiles(filename, fileoutExt)

/*
    val sumInWin = computedResults.map(_.numPoints).reduceByWindow(
        reduceFunc = _ + _, 
        invReduceFunc = _ - _, 
        Milliseconds(lookBacktime),
        Milliseconds(lookBacktime/2L)
    )
    
    sumInWin.foreachRDD(rdd => {
      assert(rdd.isEmpty || rdd.count == 1)
      
      if (!rdd.isEmpty && rdd.first == 0) {
        mylog(s"Good Bye!")
        System.exit(0)
      }
    })
*/    
    
    mylog("now!")
    Thread.sleep(60000L)

    ssc.start
    
    mylog(s"[${new Date}] starting ...")

    ssc.awaitTermination
    
    mylog(s"[${new Date}] done")
  }
  
  private def getFileExt(f: String): String = {
    val arr = f.split('.')
    if (arr.isEmpty) "" else arr.last
  }
}
