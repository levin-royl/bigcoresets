package streaming.coresets

import org.junit.Assert._
import org.junit.Test

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.sampling.SparkTestConf._
import org.apache.spark.mllib.sampling.TestUtils._

import scala.collection.JavaConverters._

import App._
import univ.ml.sparse.algorithm.SparseCoresetAlgorithm
import univ.ml.sparse.SparseWeightableVector

class MockSparseCoresetAlgorithm(sampleSize: Int) extends SparseCoresetAlgorithm {
  def takeSample(pointset: java.util.List[SparseWeightableVector]): java.util.List[SparseWeightableVector] = {
    pointset.subList(0, sampleSize);
  }
}

class SampleTakerTest {
  private def testSampleTakerById(sampleSize: Int): Unit = {
    val st = new MySampleTaker(new BaseCoresetAlgorithm(
        sparseAlg = Some(new MockSparseCoresetAlgorithm(sampleSize)))
    );

    val elms = Array(
        Domain.parseSparse("0 100 1 0.0 2 1.0 3 2.0 10 10.0"),
        Domain.parseSparse("1 100 1 0.0 2 1.0 3 2.0 10 10.0"),
        Domain.parseSparse("2 100 1 0.0 2 1.0 3 2.0 50 50.0"),
        Domain.parseSparse("3 100 1 0.0 2 1.0 3 2.0 90 90.0")
    )
    
    val elmsWithIds = elms.zipWithIndex.map(_.swap)
    val sampleRes = st.takeIds(elmsWithIds, sampleSize).toArray
    
    assertEquals(sampleSize, sampleRes.size)
    
    val expected = (0 until sampleSize).map(i => elms(i)).toArray
    val actual = (0 until sampleSize).map(i => elms(sampleRes(i))).toArray
    
    assertEquals(expected.mkString(",").toString, actual.mkString(",").toString)
  }
  
  @Test
  def testSampleTakerById(): Unit = {
    testSampleTakerById(0)
    testSampleTakerById(1)
    testSampleTakerById(2)
    testSampleTakerById(3)
    testSampleTakerById(4)
  }
}
