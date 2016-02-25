package streaming.coresets

import univ.ml.WeightedDoublePoint
import scala.ref.SoftReference

object Domain {
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
  
  type WPoint = WeightedDoublePoint

  object WPoint extends Serializable {
    def create(coords: Array[Double]): WPoint = createWeightedPoint(coords)

    def create(size: Int, pairs: Seq[(Int, Double)]): WPoint = {
      val coords = new Array[Double](size)
      pairs.par.foreach{ case(i, value) => coords(i) = value }
      create(coords)
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
    assert(arr.length >= 2 && arr.length%2 == 0)

    val rowNum = arr(0).toLong
    val size = arr(1).toInt

    val pairs = (2 until arr.length by 2).map(i => {
      val index = arr(i).toInt - 1
      val value = arr(i + 1).toDouble
      assert(index >= 0 && index < size)

      (index, value)
    })

    WPoint.create(size, pairs)
  }
}
