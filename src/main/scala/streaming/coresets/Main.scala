package streaming.coresets

import java.io.File
import scala.Iterator
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import scala.reflect.ClassTag
import org.apache.spark.streaming.Milliseconds
import univ.ml.NonUniformCoreset
import univ.ml.Sample
import univ.ml.WeightedDoublePoint
import scala.collection.JavaConverters._
import univ.ml.BaseCoreset
import univ.ml.WeightedKMeansPlusPlusClusterer
import org.apache.spark.mllib.recommendation.ALS

object Main extends Serializable {
  val hostname = "localhost"

  val port = 9999

  val sampleSize = 1024

  val estimatedNumNodesToSample = 4

  val minSizeToSample = 2*sampleSize
  
  val maxSizeToSample = 4*sampleSize
  
  val batchSecs = 10
  
  val k = 10
  
  val samplingAlg: BaseCoreset[WeightedFeatureInstance] = new NonUniformCoreset[WeightedFeatureInstance](k, sampleSize)
  
  val kmeansAlg = new WeightedKMeansPlusPlusClusterer[WeightedFeatureInstance](k)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("StreaimingCoresets")
      .setMaster(s"local[16]")
      .set("spark.ui.showConsoleProgress", "false")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.streaming.receiver.maxRate", (1024).toString)
      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//      .set("spark.streaming.backpressure.enabled", "true")

//    sparkConf.registerKryoClasses(kryoClasses)
      
    val rootDir = if (!args.isEmpty) args(0) else System.getProperty("user.home")
    val sparkCheckpointDir = s"$rootDir/spark-temp/checkpoint"
    val sparkLocal = s"$rootDir/spark-temp/local"
  
    val ssc = new StreamingContext(sparkConf, Seconds(batchSecs))
    ssc.remember(Seconds(batchSecs*2))
    ssc.checkpoint(sparkCheckpointDir)
//    ssc.sparkContext.setCheckpointDir(sparkCheckpointDir)
    
    // just testing stuff
    {
      val sc = ssc.sparkContext
      val model = ALS.train(sc.emptyRDD, 10, 100)
      
      val pre = model.predict(0, 0)
    }
    
    var stateIndexedSamples: RDD[(Layer, SampleContainer)] = null // ssc.sparkContext.emptyRDD

    val dstream = ssc.socketTextStream(hostname, port).map(raw => WeightedFeatureInstance.parse(raw))

    // never do checkpointing of dstream
//    dstream.checkpoint(Milliseconds((Long.MaxValue/timeUnit)*timeUnit))

    val indexedSamples = dstream.transform(rdd => {
      val time = System.currentTimeMillis
      
      val numPartitions = rdd.partitions.length
      val newNumPartitions = Math.max(1, numPartitions/estimatedNumNodesToSample)
      
      println(s"[$time] |rdd| = ${rdd.count} |partitions| = $numPartitions |newPartitions| = $newNumPartitions")
      
      val newIndexedSamples = rdd.mapPartitionsWithIndex((partition, it) => {
        val instances = it.toSeq

        Iterator(
            (
                Layer(0, partition/estimatedNumNodesToSample), 
                SampleContainer(instances, instances.size).performSampling(sampleSize, minSizeToSample, maxSizeToSample)
            )
        )
      }, false).flatMapValues(sc => sc) // .repartition(newNumPartitions)
      
      println(s"newIndexedSamples number of partitions = ${newIndexedSamples.partitions.length}")

      val prevIndexedSamples = stateIndexedSamples
      var currIndexedSamples = if (prevIndexedSamples != null) prevIndexedSamples.union(newIndexedSamples) else newIndexedSamples
      val nextIndexedSamples = uniteCoresetTrees(currIndexedSamples, sampleSize, minSizeToSample, maxSizeToSample)

//      nextIndexedSamples.checkpoint

      stateIndexedSamples = nextIndexedSamples.cache
      
      val loadToCache = stateIndexedSamples.count

      val deltaT = System.currentTimeMillis - time
      assert(deltaT < batchSecs*1000L, s"overtime $deltaT")
      
      println(s"next step to contain $loadToCache samples with ${stateIndexedSamples.partitions.length} partitions and took ${System.currentTimeMillis - time} milliseconds")
      
      nextIndexedSamples
    })

    indexedSamples.mapValues(_.size).print
    
    ssc.start
    ssc.awaitTermination(1000L*60L*15L)
  }
  
  // root function
  def uniteCoresetTrees(
      indexedSamples: RDD[(Layer, SampleContainer)], 
      sampleSize: Int, 
      minSizeToSample: Int,
      maxSizeToSample: Int) = 
  {
    println("uniteCoresetTrees")
    
    var currIndexedSamples = indexedSamples
    var chg = false
    
    var iter = 0
    
    do {
      println(s"uniteCoresetTrees ${iter}")
      iter+=1
    
      val nextStepIndexedSamples = currIndexedSamples.reduceByKey(_ + _)
      val markedNextIndexedSamples = nextStepIndexedSamples.flatMap{ case(layer, s) => {
        if (s.size >= minSizeToSample) {
          s.performSampling(sampleSize, minSizeToSample, maxSizeToSample).map(spl => (true, (layer.climb, spl)))
        }
        else {
          Array((false, (layer, s)))
        }
      }}.cache

      chg = !markedNextIndexedSamples.filter(_._1).isEmpty

      val nextIndexedSamples = markedNextIndexedSamples.values

      currIndexedSamples = nextIndexedSamples
    } while (chg)
      
    repartition(currIndexedSamples)
  }
  
  private def repartition(rdd: RDD[(Layer, SampleContainer)]) = {
    val requiredNumPartitions = rdd.map(_._1.nodePartition).distinct.count.toInt
    rdd.repartition(requiredNumPartitions)
  }

  // domain
  class WeightedFeatureInstance(coords: Array[Double]) extends WeightedDoublePoint(coords, 1.0, "")

  object WeightedFeatureInstance {
    def parse(raw: String) = {
      val arr = raw.split(' ')
      
      new WeightedFeatureInstance((0 until arr.length - 1).map(i => arr(i).toDouble).toArray)
    }
  }
    
  case class SampleContainer(sample: Iterable[WeightedFeatureInstance], size: Int) extends Serializable {
    require(sample.size == size)
    
//    def this(sample: Iterable[FeatureInstance]) = this(sample, sample.size)

    override def toString = sample.mkString(",")
    
    def +(other: SampleContainer) = merge(other)
    
    def merge(other: SampleContainer) = SampleContainer(sample ++ other.sample, sample.size + other.sample.size)
    
    def performSampling(sampleSize: Int, minSizeToSample: Int, maxSizeToSample: Int): Iterable[SampleContainer] = {
      assert(maxSizeToSample > 0 && maxSizeToSample >= minSizeToSample)
      
      val chunks = sample.grouped(maxSizeToSample)
      
      chunks.map(s => {
        val sSize = s.size
        if (sSize >= minSizeToSample) takeSample(s, sampleSize) else SampleContainer(s, sSize)
      }).toIterable
    }
  }
  
  case class Layer(level: Int, nodePartition: Int) extends Serializable with Comparable[Layer] {
    override def toString = s"$level::$nodePartition"
    
    override def compareTo(other: Layer) = {
      val cmpLevel = level - other.level
      if (cmpLevel != 0) cmpLevel else nodePartition - other.nodePartition
    }
    
    def climb = Layer(level + 1, nodePartition/estimatedNumNodesToSample)
  }

  // sampling
  private def takeSample(instances: Iterable[WeightedFeatureInstance], sampleSize: Int): SampleContainer = {
//    Sample(instances.map(i => (rand.nextInt, i)).toSeq.sortBy(_._1).map(_._2).take(sampleSize))
    
    if (samplingAlg != null) {
      val res = samplingAlg.takeSample(instances.toList.asJava)
      
      SampleContainer(res.asScala, sampleSize)
    }
    else {
      resorvoirSampling(Array(instances), sampleSize)
    }
  }

  private def takeMultiSample(allSamples: Iterable[SampleContainer], sampleSize: Int): SampleContainer = {
    assert(!allSamples.isEmpty)

    if (allSamples.size > 1) {
      if (samplingAlg != null) {
        val res = samplingAlg.takeSample(allSamples.flatMap(_.sample).toList.asJava)

        SampleContainer(res.asScala, sampleSize)
      }
      else {
        resorvoirSampling(allSamples.map(_.sample), sampleSize)
      }
    }
    else {
      allSamples.head
    }
  }
  
  private def resorvoirSampling(allInstances: Iterable[Iterable[WeightedFeatureInstance]], sampleSize: Int): SampleContainer = {
    val rand = new Random
    
    val resorvoir = new Array[WeightedFeatureInstance](sampleSize)
    var size = 0

    for (instances <- allInstances) {
      for (inst <- instances) {
        assert(inst != null)

        val i = size
        size += 1

        if (i >= sampleSize) {
          val j = rand.nextInt(size)

          if (j < sampleSize) {
            resorvoir(j) = inst
          }
        }
        else {
          resorvoir(i) = inst
        }
      }
    }

    val res = if (size >= sampleSize) resorvoir else resorvoir.filter(_ != null)

    SampleContainer(res, Math.min(sampleSize, size))
  }
  
  // reflection
/*    
  val kryoClasses = Array[Class[_]](
      Class.forName("scala.collection.immutable.MapLike$$anon$2")
  )
*/
}
