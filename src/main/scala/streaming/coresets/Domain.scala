package streaming.coresets

import univ.ml.WeightedDoublePoint

object Domain {
  type WPoint = WeightedDoublePoint
  
  def createWeightedPoint(coords: Array[Double]): WPoint = {
    new WPoint(coords, 1.0, "")
  }
  
  def parseDense(line: String): WPoint = {
    val arr = line.split(' ')
    assert(arr.length >= 2)
    val coords = (0 until arr.length - 1).map(i => arr(i).toDouble).toArray
    createWeightedPoint(coords)
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

    val coords = new Array[Double](size)
    pairs.par.foreach{ case(i, value) => coords(i) = value }
    createWeightedPoint(coords)
  }
}
