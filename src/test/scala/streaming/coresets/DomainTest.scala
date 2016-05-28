package streaming.coresets

import org.junit.Assert._
import org.junit.Test

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.sampling.SparkTestConf._
import org.apache.spark.mllib.sampling.TestUtils._
import streaming.coresets.Domain.SparseWPoint
import streaming.coresets.Domain.WPoint
import streaming.coresets.Domain.SparseWPoint
import univ.ml.sparse.SparseWeightableVector

import scala.collection.JavaConverters._

class DomainTest {
  private def createPoint(points: Iterable[(Int, Double)], dim: Int): WPoint = {
    val javaVec = new SparseWeightableVector(
        points.map(ent => (new java.lang.Integer(1), new java.lang.Double(1.0))).toMap.asJava, 
        1.0, 
        dim
    )
    
    new SparseWPoint(javaVec)
  }
  
  @Test
  def testToVector(): Unit = {
    val p: WPoint = createPoint(Seq(
        (1, 1.0)
    ), 10)
    
    assertTrue(p.isSparse)
    assertEquals(10, p.toVector.size)
    assertEquals(1, p.toVector.numNonzeros)
    assertEquals("(10,[1],[1.0])", p.toVector.toString)
  }
}
