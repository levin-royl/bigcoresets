package streaming.coresets

import univ.ml.WeightedDoublePoint
import scala.ref.SoftReference
import univ.ml.sparse.SparseWeightableVector
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.JavaConverters._
import java.util.HashMap
import scala.util.Random

object Domain {
  case class ComputedResult(
      points: Array[Vector],
      numPoints: Long,
      sampleSize: Int,
      algName: String,
      time: Long = System.currentTimeMillis
  ) extends Serializable
  
  def createWeightedPoint(coords: Array[Double], w: Double = 1.0): WeightedDoublePoint = {
    new WeightedDoublePoint(coords, w, "")
  }

/*  
  trait AbstractWPoint extends Serializable {
    def toWeightedDoublePoint(): WeightedDoublePoint
  }
  
  class WrapperWPoint(wpoint: WeightedDoublePoint) extends AbstractWPoint {
    override def toWeightedDoublePoint(): WeightedDoublePoint = wpoint
  }
  
  class LazyWPoint(size: Int, pairs: Seq[(Int, Double)], weight: Double) extends AbstractWPoint {
    private var cachedWPoint: SoftReference[WeightedDoublePoint] = new SoftReference[WeightedDoublePoint](null)
    
    private def createWeightedDoublePoint(): WeightedDoublePoint = {
      val coords = new Array[Double](size)
      pairs.par.foreach{ case(i, value) => coords(i) = value }
      createWeightedPoint(coords, weight)
    }
    
    override def toWeightedDoublePoint(): WeightedDoublePoint = {
      this.synchronized {
        val wp = cachedWPoint.get
        
        if (wp.isEmpty) {
          val newP = createWeightedDoublePoint()
          cachedWPoint = new SoftReference(newP)
          newP
        }
        else wp.get
      }
    }
  }
*/
  
  implicit class WeightedDoublePointHelper(p: WeightedDoublePoint) extends Serializable {
    def toVector(): Vector = Vectors.dense(p.getPoint)
  }
  
  implicit class SparseWeightableVectorHelper(p: SparseWeightableVector) extends Serializable {
    def toVector(): Vector = {
      val res = Vectors.sparse(
          p.getDimension, 
          p.sparseIterator.asScala.map(ent => (ent.getIndex, ent.getValue)).toSeq
      )
      
      res
    }
  }
  
  trait WPoint extends Serializable {
    def isSparse: Boolean
    
    def toWeightedDoublePoint: WeightedDoublePoint
    
    def toSparseWeightableVector: SparseWeightableVector
    
    def toVector: Vector
    
    override def toString: String = toVector.toString
    
    override def hashCode(): Int = toVector.hashCode
    
    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[WPoint]) {
        val that = other.asInstanceOf[WPoint]
        (this eq that) || toVector.equals(that.toVector)
      }
      else false
    }
  }
  
  class DenseWPoint(inner: WeightedDoublePoint) extends WPoint {
    override def isSparse: Boolean = false
    
    override def toWeightedDoublePoint: WeightedDoublePoint = inner
    
    override def toSparseWeightableVector: SparseWeightableVector = {
      throw new UnsupportedOperationException("not sparse")
    }
    
    override def toVector: Vector = Vectors.dense(toWeightedDoublePoint.getPoint)
  }
  
  class SparseWPoint(inner: SparseWeightableVector) extends WPoint {
    override def isSparse: Boolean = true
    
    override def toWeightedDoublePoint: WeightedDoublePoint = {
      throw new UnsupportedOperationException("not dense")
    }
    
    override def toSparseWeightableVector: SparseWeightableVector = inner

    override def toVector: Vector = {
      val res = Vectors.sparse(
          inner.getDimension, 
          inner.sparseIterator.asScala.map(ent => {
            (ent.getIndex, ent.getValue)
          }).toSeq
      )
      
      res
    }
  }
  
  object WPoint extends Serializable {
    def apply(inner: WeightedDoublePoint) = new DenseWPoint(inner)
    
    def apply(inner: SparseWeightableVector) = new SparseWPoint(inner)
    
    def create(coords: Array[Double]): WPoint = new DenseWPoint(createWeightedPoint(coords))

    def create(size: Int, pairs: Seq[(Int, Double)]): WPoint = {
      val map = new HashMap[java.lang.Integer, java.lang.Double](pairs.length)
      pairs.foreach(p => map.put(p._1, p._2))
      WPoint(new SparseWeightableVector(map, size))

/*      
      val coords = new Array[Double](size)
      pairs.par.foreach{ case(i, value) => coords(i) = value }
      create(coords)
*/
    }
  }
  
/*  
  object WPoint extends Serializable {
    def create(coords: Array[Double]): AbstractWPoint = 
      new WrapperWPoint(createWeightedPoint(coords))

    def create(size: Int, pairs: Seq[(Int, Double)]): AbstractWPoint = 
      new LazyWPoint(size, pairs, 1.0)
    
    def create(point: WeightedDoublePoint): AbstractWPoint = {
      val zc = point.getPoint.filter(_ == 0.0).size.toDouble
      val c = point.getPoint.size.toDouble
      
      if (zc/c < 0.5) {
        new LazyWPoint(
            point.getPoint.length, 
            (0 until point.getPoint.length).map(i => (i, point.getPoint()(i))),
            point.getWeight
        )
      }
      else {
        new WrapperWPoint(point)
      }
    }
  }
*/  

  def parseDense(line: String): WPoint = {
    val arr = line.split(' ')
    assert(arr.length >= 2)
    val coords = (0 until arr.length - 1).map(i => arr(i).toDouble).toArray
    WPoint.create(coords)
  }
  
  def parseSparse(line: String): WPoint = {
    val arr = line.split(' ')
    assert(arr.length >= 2 && arr.length%2 == 0, s"${arr.mkString(",")}")

    val rowNum = arr(0).toLong
    val size = arr(1).toInt

    val pairs = (2 until arr.length by 2).map(i => {
      val index = arr(i).toInt - 1
      val value = arr(i + 1).toDouble
      assert(index >= 0 && index < size)

      (index, value)
    })

    val res = WPoint.create(size, pairs)
    
    res
  }
}
