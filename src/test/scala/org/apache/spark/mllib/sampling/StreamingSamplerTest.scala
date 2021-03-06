package org.apache.spark.mllib.sampling

import org.apache.spark.mllib.sampling.RDDLike.rddLikeToPairRDDLikeFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.junit.Assert._
import org.junit.Test

import SparkTestConf.sc
import TestUtils.TestHelperRDDLike
import streaming.coresets.BaseCoresetAlgorithm
import univ.ml.NonUniformCoreset
import univ.ml.WeightedDoublePoint
import streaming.coresets.Domain.WPoint
import streaming.coresets.MySampleTaker
import scala.util.Random
import univ.ml.sparse.algorithm.SparseKmeansCoresetAlgorithm

class StreamingSamplerTest {
  val sampler = new TreeSampler(SamplerConfig(2, 5, true), new TestSampleTaker)

  val rdd = sc.makeRDD(0 until 110, 11)
  assertEquals(11, rdd.partitions.length)

  @Test
  def testKMeansBulk(): Unit = {
    val k = 3
    val sampleSize = 3
    
    val st = new BaseCoresetAlgorithm(
        denseAlg = Some(new NonUniformCoreset[WeightedDoublePoint](k, sampleSize))
    )
    
    val sampler = new TreeSampler[WPoint](
      SamplerConfig(2, sampleSize, true),
      new MySampleTaker(st)
    )
    
    val r = new Random
    
    val lows = sc.makeRDD((0 until 1000).map(i => -100000 - r.nextInt(100)))
    val mids = sc.makeRDD((0 until 1000).map(i => r.nextInt(100)))
    val highs = sc.makeRDD((0 until 1000).map(i => 100000 + r.nextInt(100)))
    val rdd = lows.union(mids).union(highs).map(i => WPoint.create(Array[Double](i)))

    val res = sampler.sample(rdd)
    
    println(s"testKMeansBulk")
    res.foreach(println)
  }
  
  @Test
  def testKMeansCoresetBulk(): Unit = {
    val k = 3
    val sampleSize = 3
    
    val st = new BaseCoresetAlgorithm(
        sparseAlg = Some(new SparseKmeansCoresetAlgorithm(sampleSize))
    )
    
    val sampler = new TreeSampler[WPoint](
      SamplerConfig(2, sampleSize, true),
      new MySampleTaker(st)
    )
    
    val r = new Random
    
    val lows = sc.makeRDD((0 until 1000).map(i => -100000 - r.nextInt(100)))
    val mids = sc.makeRDD((0 until 1000).map(i => r.nextInt(100)))
    val highs = sc.makeRDD((0 until 1000).map(i => 100000 + r.nextInt(100)))
    val rdd = lows.union(mids).union(highs).map(i => WPoint.create(1, Seq((0, i.toDouble))))

    val res = sampler.sample(rdd)
    
    println(s"testKMeansCoresetBulk")
    res.foreach(println)
  }
  
  @Test
  def testFlatSample(): Unit = {
    val flatSample = sampler.flatTreeSample(rdd)
    assertEquals(11, flatSample.count)
    
    val levels = flatSample.map(_._1.level).distinct
    
    assertEquals(1, levels.count)
    assertEquals(0, levels.first)

    val nodePartitions = flatSample.map(_._1.nodePartition)
    
    val byValueCount = nodePartitions.countByValue
    
    assertEquals(2, byValueCount.get(0).get)
    assertEquals(2, byValueCount.get(1).get)
    assertEquals(2, byValueCount.get(2).get)
    assertEquals(2, byValueCount.get(3).get)
    assertEquals(2, byValueCount.get(4).get)
    assertEquals(1, byValueCount.get(5).get)
    
    val sampleContinerArr = flatSample.map(_._2.sample).collect
    
    assertEquals(11, sampleContinerArr.length)

    assertArrayEquals(Array.range(0, 10, 2), sampleContinerArr(0).toArray)
    assertArrayEquals(Array.range(10, 20, 2), sampleContinerArr(1).toArray)
    assertArrayEquals(Array.range(20, 30, 2), sampleContinerArr(2).toArray)
    assertArrayEquals(Array.range(30, 40, 2), sampleContinerArr(3).toArray)
    assertArrayEquals(Array.range(40, 50, 2), sampleContinerArr(4).toArray)
    assertArrayEquals(Array.range(50, 60, 2), sampleContinerArr(5).toArray)
    assertArrayEquals(Array.range(60, 70, 2), sampleContinerArr(6).toArray)
    assertArrayEquals(Array.range(70, 80, 2), sampleContinerArr(7).toArray)
    assertArrayEquals(Array.range(80, 90, 2), sampleContinerArr(8).toArray)
    assertArrayEquals(Array.range(90, 100, 2), sampleContinerArr(9).toArray)
    assertArrayEquals(Array.range(100, 110, 2), sampleContinerArr(10).toArray)    
  }
  
  @Test
  def testTreeSample(): Unit = {
    val tree = sampler.treeSample(new RDDLikeIterable(sampler.flatTreeSample(rdd).collect))
    val treeMap = tree.collect.toMap
    
    assertEquals(3, treeMap.size)
    
    val layer0 = treeMap.get(Layer(0, 5)).get.sample
    val layer1 = treeMap.get(Layer(1, 2)).get.sample
    val layer3 = treeMap.get(Layer(3, 0)).get.sample
    
    assertArrayEquals(Array(100, 102, 104, 106, 108), layer0.toArray)
    assertArrayEquals(Array(80, 84, 88, 92, 96), layer1.toArray)
    assertArrayEquals(Array(0, 16, 32, 48, 64), layer3.toArray)
  }
  
  @Test
  def testJoin(): Unit = {
    val s1 = Seq(1, 2, 3, 4, 5).map(i => (i, i.toString))
    val s2 = Seq(2, 3, 5, 5).map(i => (i, i.toDouble))
    
    val rdd1 = sc.makeRDD(s1)
    val rdd2 = sc.makeRDD(s2)
    
    testJoin(new RDDLikeWrapper(rdd1.join(rdd2)))
    
    val itLike1 = new RDDLikeIterable(s1)
    val rddLike1 = new RDDLikeWrapper(rdd1)
    
    val itLike2 = new RDDLikeIterable(s2)
    val rddLike2 = new RDDLikeWrapper(rdd2)
    
    testJoin(itLike1.join(itLike2))
    testJoin(rddLike1.join(rddLike2))
    
    testJoin(itLike1.join(rddLike2))
    testJoin(rddLike1.join(itLike2))
  }
  
  private def testJoin(joinedRDD: RDDLike[(Int, (String, Double))]): Unit = {
    assertEquals(4L, joinedRDD.count)
    
    assertEquals(("2", 2.0), joinedRDD.filter(_._1 == 2).values.single)
    assertEquals(("3", 3.0), joinedRDD.filter(_._1 == 3).values.single)
    assertEquals(Array(("5", 5.0), ("5", 5.0)).mkString(","), joinedRDD.filter(_._1 == 5).values.collect.mkString(","))
  }
}


// mock objects:
class TestSampleTaker extends SampleTaker[Int] {
  private def smallest(iter: Iterable[Int], sampleSize: Int): Iterable[Int] = {
    val arr = iter.toArray.sortBy(i => i)
    val step = arr.length/sampleSize
    
    (0 until arr.length by step).map(i => arr(i))
  }

  override def take(elms: Iterable[Int], sampleSize: Int): Iterable[Int] = {
    val res = if (elms.size > sampleSize) {
      smallest(elms, sampleSize)
    }
    else elms

    res
  }

  override def takeIds(elmsWithIds: Iterable[(Int, Int)], sampleSize: Int): Set[Int] = {
    val withIds = elmsWithIds.map{ case(id, elm) => elm }
    val sample = take(withIds, sampleSize)

    sample.toSet
  }
}
