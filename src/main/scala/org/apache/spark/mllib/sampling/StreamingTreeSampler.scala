package org.apache.spark.mllib.sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Seconds
import scala.reflect.ClassTag
import org.apache.spark.Logging

import scala.collection.mutable.HashMap

object GenUtil {
  def mylog(msg: String): Unit = {
    println(s"${new java.util.Date} --- $msg")
  }
}

// public domain classes
case class SamplerConfig(
    numNodesToSample: Int,
    sampleSize: Int,
    localRDDs: Boolean
) extends Serializable {
  val expectedInputSampleSize = numNodesToSample*sampleSize
  
  def createRDDLike[T](rdd: RDD[T]): RDDLike[T] = {
    if (localRDDs) {
      new RDDLikeIterable(rdd.collect)
    }
    else {
      new RDDLikeWrapper(rdd)
    }
  }
}

trait SampleTaker[T] extends Serializable {
  def take(elms: Iterable[T], sampleSize: Int): Iterable[T]

  def takeIds(elmsWithIds: Iterable[(Int, T)], sampleSize: Int): Iterable[Int]
}

object TreeSampler extends Serializable with Logging {
	def info(msg: String): Unit = GenUtil.mylog(msg)
//  def info(msg: String): Unit = logWarning(msg)
}

import org.apache.spark.mllib.sampling.TreeSampler._
import org.apache.spark.SparkContext
import java.util.Date
import java.util.Date

// RDDLike stuff
trait RDDLike[T] extends Serializable {
  def isEmpty: Boolean
  
  def count: Long
  
  def numPartitions: Int
  
  def first: T

  def collect: Iterable[T]
  
  def distinct: RDDLike[T]
  
  def zipWithIndex: RDDLike[(T, Long)]

  def union(other: RDDLike[T]): RDDLike[T]
  
  def filter(f: T => Boolean): RDDLike[T]
  
  def map[U: ClassTag](f: T => U): RDDLike[U]

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDDLike[U]
  
  def repartition(numPartitions: Int): RDDLike[T]
  
  def cache: RDDLike[T]

  def checkpoint: Unit
  
  def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = numPartitions)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDDLike[T]
}

class RDDLikeIterable[T](self: Iterable[T]) extends RDDLike[T] {
  private[sampling] def getSelf = self
  
  def isEmpty: Boolean = self.isEmpty
  
  def numPartitions: Int = 1
  
  def count: Long = self.size
  
  def first: T = self.head
  
  def collect: Iterable[T] = self

  def distinct: RDDLike[T] = new RDDLikeIterable(self.toSet)

  def zipWithIndex: RDDLike[(T, Long)] = 
    new RDDLikeIterable(self.zipWithIndex.map(p => (p._1, p._2.toLong)))

  def union(other: RDDLike[T]): RDDLike[T] = {
    require(other.isInstanceOf[RDDLikeIterable[T]])
    val that = other.asInstanceOf[RDDLikeIterable[T]]
    new RDDLikeIterable(self ++ that.getSelf)
  }
  
  def filter(f: T => Boolean): RDDLike[T] = new RDDLikeIterable(self.filter(f))

  def map[U: ClassTag](f: T => U): RDDLike[U] = new RDDLikeIterable(self.map(f))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDDLike[U] =
    new RDDLikeIterable(self.flatMap(f))
  
  def repartition(numPartitions: Int): RDDLike[T] = this
  
  def cache: RDDLike[T] = this
  
  def checkpoint: Unit = {}
  
  def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = numPartitions)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDDLike[T] = {

    val resSeq = self.toSeq.sortBy(f)
    val finalResSeq = if (ascending) resSeq else resSeq.reverse
    
    new RDDLikeIterable(finalResSeq)
  }
}

class RDDLikeWrapper[T](self: RDD[T]) extends RDDLike[T] {
  private[sampling] def getSelf = self
  
  def isEmpty: Boolean = self.isEmpty
  
  def count: Long = self.count
  
  def numPartitions: Int = self.partitions.length
  
  def first: T = self.first
  
  def collect: Iterable[T] = self.collect

  def distinct: RDDLike[T] = new RDDLikeWrapper(self.distinct)

  def zipWithIndex: RDDLike[(T, Long)] = new RDDLikeWrapper(self.zipWithIndex)

  def union(other: RDDLike[T]): RDDLike[T] = {
    require(other.isInstanceOf[RDDLikeWrapper[T]])
    val that = other.asInstanceOf[RDDLikeWrapper[T]]
    new RDDLikeWrapper(self.union(that.getSelf))
  }
  
  def filter(f: T => Boolean): RDDLike[T] = new RDDLikeWrapper(self.filter(f))

  def map[U: ClassTag](f: T => U): RDDLike[U] = new RDDLikeWrapper(self.map(f))

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDDLike[U] =
    new RDDLikeWrapper(self.flatMap(f))
  
  def repartition(numPartitions: Int): RDDLike[T] = 
    new RDDLikeWrapper(getSelf.repartition(numPartitions))
  
  def cache: RDDLike[T] = new RDDLikeWrapper(getSelf.cache)
  
  def checkpoint: Unit = getSelf.checkpoint
  
  def sortBy[K](
    f: (T) => K,
    ascending: Boolean = true,
    numPartitions: Int = numPartitions)
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDDLike[T] = {

    new RDDLikeWrapper(self.sortBy(f, ascending, numPartitions))
  }
}

class PairRDDLikeFunctions[K, V](self: RDDLike[(K, V)])
  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
extends Serializable {
  private def selfIter: Option[Iterable[(K, V)]] = {
    if (self.isInstanceOf[RDDLikeIterable[(K, V)]]) {
      val si = self.asInstanceOf[RDDLikeIterable[(K, V)]]
      Some(si.getSelf)
    }
    else None
  }
  
  private def selfRDD: Option[RDD[(K, V)]] = {
    if (self.isInstanceOf[RDDLikeWrapper[(K, V)]]) {
      val srdd = self.asInstanceOf[RDDLikeWrapper[(K, V)]]
      Some(srdd.getSelf)
    }
    else None
  }
  
  private def op[RT](
      selfIterFunc: (Iterable[(K, V)] => Iterable[RT]), 
      selfRddFunc: (RDD[(K, V)] => RDD[RT])): RDDLike[RT] = {
    val oSelfIter = selfIter
    
    if (!oSelfIter.isDefined) {
      val oSelfRDD = selfRDD
      assert(oSelfRDD.isDefined)
      new RDDLikeWrapper(selfRddFunc(oSelfRDD.get))
    }
    else {
      new RDDLikeIterable(selfIterFunc(oSelfIter.get))
    }
  }
  
  def keys: RDDLike[K] = op(iter => iter.map(_._1), rdd => rdd.keys)
  
  def values: RDDLike[V] = op(iter => iter.map(_._2), rdd => rdd.values)
  
  def mapValues[U](f: V => U): RDDLike[(K, U)] = 
    op(iter => iter.map(pair => (pair._1, f(pair._2))), rdd => rdd.mapValues(f))

  def join[U](other: RDDLike[(K, U)]): RDDLike[(K, (V, U))] = {
    if (self.isInstanceOf[RDDLikeIterable[(K, V)]] != other.isInstanceOf[RDDLikeIterable[(K, U)]]) {
      require(self.isInstanceOf[RDDLikeWrapper[(K, V)]] || other.isInstanceOf[RDDLikeWrapper[(K, U)]])
      
      if (self.isInstanceOf[RDDLikeIterable[(K, V)]]) {
        assert(other.isInstanceOf[RDDLikeWrapper[(K, U)]])
        val rddSc = other.asInstanceOf[RDDLikeWrapper[(K, U)]].getSelf.sparkContext
        return new RDDLikeWrapper(rddSc.makeRDD(self.collect.toSeq)).join(other)
      }
      else {
        assert(self.isInstanceOf[RDDLikeWrapper[(K, V)]])
        assert(other.isInstanceOf[RDDLikeIterable[(K, U)]])
        val rddSc = self.asInstanceOf[RDDLikeWrapper[(K, V)]].getSelf.sparkContext
        return self.join(new RDDLikeWrapper(rddSc.makeRDD(other.collect.toSeq)))
      }
    }
    else if (other.isInstanceOf[RDDLikeIterable[(K, U)]]) {
      require(self.isInstanceOf[RDDLikeIterable[(K, V)]])
      
      val me = selfIter.get
      val that = other.asInstanceOf[RDDLikeIterable[(K, U)]]
      val thatMap = that.getSelf.groupBy(_._1)
      
      val resIt = me.flatMap{ case(key, v1) => {
        val those = thatMap.get(key).getOrElse(Iterable.empty)
        those.map{ case(_, v2) => (key, (v1, v2)) }
      }}
      
      return new RDDLikeIterable(resIt)
    }
    else {
      require(self.isInstanceOf[RDDLikeWrapper[(K, V)]])
      require(other.isInstanceOf[RDDLikeWrapper[(K, U)]])

      val me = selfRDD.get
      val that = other.asInstanceOf[RDDLikeWrapper[(K, U)]].getSelf
      val resRDD = me.join(that)
      
      return new RDDLikeWrapper(resRDD)
    }
  }
    
  private def iterReduceByKey(iter: Iterable[(K, V)], func: (V, V) => V): Iterable[(K, V)] = {
    val map = new HashMap[K, V]

    for ((key, value) <- iter) {
      val prevValue = map.get(key)
      val newValue = if (prevValue.isDefined) {
        func(prevValue.get, value)
      }
      else value
      
      map.put(key, newValue)
    }

    map
  }

  def reduceByKey(func: (V, V) => V): RDDLike[(K, V)] = 
    op(iter => iterReduceByKey(iter, func), rdd => rdd.reduceByKey(func))
}

object RDDLike {
  implicit def rddLikeToPairRDDLikeFunctions[K, V](rdd: RDDLike[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairRDDLikeFunctions[K, V] = {
    new PairRDDLikeFunctions(rdd)
  }

  def toRDD[T: ClassTag](rddLike: RDDLike[T], sc: SparkContext): RDD[T] = {
    if (rddLike.isInstanceOf[RDDLikeWrapper[_]]) {
      rddLike.asInstanceOf[RDDLikeWrapper[T]].getSelf
    }
    else {
      sc.makeRDD(rddLike.collect.toSeq)
    }
  }
}

// tree sampler
class TreeSampler[T](
    config: SamplerConfig, 
    sampleTaker: SampleTaker[T])(implicit m: ClassTag[T]) extends Serializable {
  def sample(rdd: RDD[T]): Iterable[T] = {
    val flatRDD = flatTreeSample(rdd)
    val tree = treeSample(config.createRDDLike(flatRDD))

    sampleFromTree(tree)
  }
  
  private[sampling] 
  def sampleFromTree(rdd: RDDLike[(Layer, SampleContainer[T])]): Iterable[T] = {
    val elms = rdd.flatMap{ case(_, sc) => sc.sample }
    sampleTaker.take(elms.collect, config.sampleSize)
  }
  
  private[sampling] 
  def sampleFromTreeToRDD(rdd: RDDLike[(Layer, SampleContainer[T])]): RDDLike[T] = {
    val elms = rdd.flatMap{ case(_, sc) => sc.sample }
      .zipWithIndex.map{ case(elm, id) => (id.toInt, elm) }

    val ids = new RDDLikeIterable(sampleTaker.takeIds(elms.collect, config.sampleSize)).zipWithIndex

    ids.join(elms).values.sortBy(_._1).values
  }
  
  private[sampling] 
  def flatTreeSample(rdd: RDD[T]): RDD[(Layer, SampleContainer[T])] = {
//    val origNumPartitions = rdd.partitions.length

    rdd.mapPartitionsWithIndex((partition, it) => {
      val instances = it.toSeq

      Iterator(
          (
              Layer(0, partition/config.numNodesToSample), 
              SampleContainer.create(instances).performSampling(config, sampleTaker)
          )
      )
    }, false).flatMapValues(sc => sc) // .repartition(origNumPartitions)
  }
  
  private[sampling] 
  def treeSample(rdd: RDDLike[(Layer, SampleContainer[T])]): RDDLike[(Layer, SampleContainer[T])] = {
    if (!rdd.isEmpty) {
      var currIndexedSamples = rdd
      var chg = false
      
      var iter = 0

      do {
        info(s"treeSample reduce iteration ${iter} (numPartitions=${currIndexedSamples.numPartitions})")
        iter+=1
        
        val prevIndexedSamples = currIndexedSamples
        val nextStepIndexedSamples = prevIndexedSamples.reduceByKey(_ + _)
        
        val markedNextIndexedSamples = nextStepIndexedSamples.flatMap{ case(layer, s) => {
          if (s.size >= config.expectedInputSampleSize) {
            s.performSampling(config, sampleTaker).map(spl => (true, (climb(layer), spl)))
          }
          else {
            Array((false, (layer, s)))
          }
        }}.cache

        chg = !markedNextIndexedSamples.keys.filter(g => g).isEmpty
        val nextIndexedSamples = markedNextIndexedSamples.values
        
        currIndexedSamples = nextIndexedSamples.cache
      } while (chg && currIndexedSamples.count > 1L)

      info(s"treeSample reduce completed after ${iter} iterations")

      currIndexedSamples
    }
    else {
      rdd
    }
  }

/*  
  private def repartition(rdd: RDDLike[(Layer, SampleContainer[T])]) = {
    val requiredNumPartitions = rdd.map(_._1.nodePartition).distinct.count.toInt
    rdd.repartition(requiredNumPartitions)
  }
*/

  private def climb(layer: Layer): Layer = {
    Layer(layer.level + 1, layer.nodePartition/config.numNodesToSample)
  }
}

class StreamingTreeSampler[T](
    config: SamplerConfig, 
    sampleTaker: SampleTaker[T], 
    batchSecs: Int)(implicit m: ClassTag[T]) extends Serializable {

  def defaultSample(dstream: DStream[T]): DStream[T] = {
    sample(dstream, (rdd, s) => s)
  }
  
  def sample[RT: ClassTag](
      dstream: DStream[T], 
      processSample: (RDD[T], RDDLike[T]) => RDDLike[RT]): DStream[RT] = {

    val ssc = dstream.context
//    ssc.remember(Seconds(batchSecs*2))

    var stateTreeSample: RDDLike[(Layer, SampleContainer[T])] = null
    val treeSampler = new TreeSampler[T](config, sampleTaker)

    dstream.transform(rdd => {
      val before = System.currentTimeMillis

      val origNumPartitions = rdd.partitions.length
      val currFlatTreeSample = config.createRDDLike(treeSampler.flatTreeSample(rdd))
      val nextRawTreeSample = if (stateTreeSample != null) {
        stateTreeSample.union(currFlatTreeSample).repartition(origNumPartitions)
      } else currFlatTreeSample
      
      val currTreeSample = treeSampler.treeSample(nextRawTreeSample).repartition(origNumPartitions)
      
      currTreeSample.checkpoint
      val cachedCurrTreeSample = currTreeSample.cache
      stateTreeSample = cachedCurrTreeSample

      val res = processSample(rdd, treeSampler.sampleFromTreeToRDD(cachedCurrTreeSample))

      val deltaT = System.currentTimeMillis - before

//      require(deltaT < 1000L*batchSecs, s"$deltaT")
      
      val rdyRDD: RDD[RT] = RDDLike.toRDD(res, rdd.sparkContext)
      
      info(s"stream processing duration is $deltaT ms")
      
      rdyRDD
    })
  }
  
  @Deprecated
  def oldSample(
      dstream: DStream[T], 
      preCoreset: RDD[T] => Unit = null,
      postCoreset: Iterable[T] => Unit = null): DStream[T] = {
    val ssc = dstream.context
//    ssc.remember(Seconds(batchSecs*2))

    var stateTreeSample: RDDLike[(Layer, SampleContainer[T])] = null
    val treeSampler = new TreeSampler[T](config, sampleTaker)

    dstream.transform(rdd => {
      val before = System.currentTimeMillis
      
      if (preCoreset != null) {
        preCoreset(rdd)
      }

      val origNumPartitions = rdd.partitions.length
      val currFlatTreeSample = config.createRDDLike(treeSampler.flatTreeSample(rdd))
      val nextRawTreeSample = if (stateTreeSample != null) {
        stateTreeSample.union(currFlatTreeSample).repartition(origNumPartitions)
      } else currFlatTreeSample
      
      val currTreeSample = treeSampler.treeSample(nextRawTreeSample).repartition(origNumPartitions)
      
      currTreeSample.checkpoint
      val cachedCurrTreeSample = currTreeSample.cache
      stateTreeSample = cachedCurrTreeSample

      val res = treeSampler.sampleFromTreeToRDD(cachedCurrTreeSample)

      if (postCoreset != null) {
        postCoreset(res.collect)
      }
      
      val deltaT = System.currentTimeMillis - before

//      require(deltaT < 1000L*batchSecs, s"$deltaT")

      val rdyRDD = if (res.isInstanceOf[RDDLikeWrapper[_]]) {
        res.asInstanceOf[RDDLikeWrapper[T]].getSelf
      }
      else {
        rdd.sparkContext.makeRDD(res.collect.toSeq)
      }
      
      info(s"stream processing duration is $deltaT ms")
      
      rdyRDD
    })
  }  
}

// private domain classes
private object SampleContainer extends Serializable {
  def create[T](sample: Iterable[T], size: Int)(implicit m: ClassTag[T]): SampleContainer[T] = {
    require(sample.isInstanceOf[Serializable])
    require(sample.size == size)

    new SampleContainer(sample, size)
  }

  def create[T](sample: Iterable[T])(implicit m: ClassTag[T]): SampleContainer[T] = {
    create(sample, sample.size)
  }
}

private case class SampleContainer[T](
    sample: Iterable[T], 
    size: Int)(implicit m: ClassTag[T]) extends Serializable {
  def + (other: SampleContainer[T]): SampleContainer[T] = merge(other)
  
  def merge(other: SampleContainer[T]): SampleContainer[T] = {
    val newArr = new Array[T](size + other.size)
    var i = 0
    
    for (elm <- sample) {
      newArr(i) = elm
      i += 1
    }
    
    for (elm <- other.sample) {
      newArr(i) = elm
      i += 1
    }
    
    SampleContainer.create(newArr, newArr.length)
//    SampleContainer.create(sample ++ other.sample, sample.size + other.sample.size)
  }
  
  def performSampling(
      config: SamplerConfig, 
      sampleTaker: SampleTaker[T]): Iterable[SampleContainer[T]] = {
    val inputSampleSize = config.numNodesToSample*config.sampleSize
    val chunks: Iterator[Iterable[T]] = if (inputSampleSize > sample.size) {
      sample.grouped(inputSampleSize) 
    } else {
      Iterator(sample)
    }

    val res = chunks.map(s => {
      val cs = s.toList
      val sSize = cs.size
      
      if (sSize >= inputSampleSize) {
        require(sSize > config.sampleSize)
        val theSample = sampleTaker.take(cs, config.sampleSize)
        require(theSample.size < sSize, s"asked for sample size ${config.sampleSize} but got ${theSample.size}")
        SampleContainer.create(theSample, theSample.size)
      }
      else {
        SampleContainer.create(cs, sSize)
      }
    }).toIterable

    res
  }

  override def toString: String = sample.mkString(",")
  
//  override def size: Int = theSize
  
//  override def iterator: Iterator[T] = sample.iterator
}

private case class Layer(level: Int, nodePartition: Int) extends Serializable with Comparable[Layer] {
  override def toString: String = s"$level::$nodePartition"
  
  override def compareTo(other: Layer): Int = {
    val cmpLevel = level - other.level
    if (cmpLevel != 0) cmpLevel else nodePartition - other.nodePartition
  }
}
