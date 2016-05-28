package streaming.coresets

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import java.util.UUID

object GenUtil {
  implicit class RDDHelper[T: ClassTag](rdd: RDD[T]) {
    def materialize(): RDD[T] = {
      val sc = rdd.sparkContext
      require(sc.getCheckpointDir.isDefined)
      val path = sc.getCheckpointDir.get
      val rddPath = s"${path}/checkpoint-rdd-${UUID.randomUUID.toString}"
      rdd.saveAsObjectFile(rddPath)
      sc.objectFile[T](rddPath)
    }
  }
}
