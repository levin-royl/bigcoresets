package com.ibm.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Collection
import java.net.URL
import org.reflections.util.ClasspathHelper
import java.util.ArrayList
import scala.collection.JavaConverters._
import org.reflections.Reflections
import org.reflections.util.ConfigurationBuilder
import org.reflections.scanners.SubTypesScanner
import java.util.Date
import java.io.File

object Config {
  private lazy val defaultSparkConf = new SparkConf()
    .set("spark.app.name", "UnitTest")
    .set("spark.ui.showConsoleProgress", "false")
//    .set("spark.master", "local[1]")
    .set("spark.master", "local[2]") // debug on more than one thread?
    
  private var sparkConf: Option[SparkConf] = None
  
  private var sparkContext: Option[SparkContext] = None
  
  def setConf(conf: SparkConf) { sparkConf = Some(conf) }
  
  def setSc(sc: SparkContext) { sparkContext = Some(sc) }
  
  private def packages(iter: Iterable[String]): Collection[URL] = {
    val res = iter.flatMap(ClasspathHelper.forPackage(_).asScala)
    val arr = new ArrayList[URL](res.size)
    res.foreach(url => arr.add(url))
    arr
  }
  
  private def loadClass(name: String): Class[_] = {
    Class.forName(name, false, getClass().getClassLoader());
  }

  private def loadArrayClass(name: String): Class[_] = {
    java.lang.reflect.Array.newInstance(loadClass(name), 0).getClass
  }
  
  private def loadMatrixClass(name: String): Class[_] = {
    java.lang.reflect.Array.newInstance(loadClass(name), 0, 0).getClass
  }
  
  def initSpark(
      appName: String, 
      sparkParams: Map[String, String],
      kryoDomainPackages: Array[String] = Array.empty,
      kryoDomainClasses: Array[String] = Array.empty)
  {
    val myConf = new SparkConf()
      .set("spark.app.name", appName)
      .set("spark.ui.showConsoleProgress", "false")

    sparkParams.foreach(kv => myConf.set(kv._1, kv._2))
    
    if (
        sparkParams.getOrElse("spark.serializer", "").equals("org.apache.spark.serializer.KryoSerializer") &&
        sparkParams.getOrElse("spark.kryo.registrationRequired", "").equalsIgnoreCase("true")
       ) 
    {
      val generalPackages = Array[String](
//          "scala.collection.immutable"
//          "org.apache.spark.util",
//          "org.apache.spark.util.collection"
      )

      val reflections = new Reflections(new ConfigurationBuilder()
        .setUrls(packages(generalPackages.union(kryoDomainPackages)))
        .setScanners(new SubTypesScanner(false)));
      
      val generalClasses = Array[Class[_]](
          classOf[Array[String]],
          loadClass("scala.Enumeration$Val"),
          loadClass("scala.reflect.ClassTag$$anon$1"),
          loadClass("scala.reflect.ManifestFactory$$anon$1"),
          loadClass("scala.None$"),
          loadClass("scala.Tuple2"),
          loadArrayClass("scala.Tuple2"),
          loadClass("scala.Tuple3"),
          loadArrayClass("scala.Tuple3"),
          loadClass("scala.collection.immutable.Nil$"),
          loadClass("scala.collection.immutable.$colon$colon"),
          loadClass("scala.collection.immutable.Set$EmptySet$"),
          loadClass("scala.collection.mutable.ArrayBuffer"),
          loadArrayClass("scala.collection.mutable.ArrayBuffer"),

          loadClass("org.apache.spark.util.StatCounter"),
          loadClass("org.apache.spark.util.collection.CompactBuffer"),
          loadArrayClass("org.apache.spark.util.collection.CompactBuffer"),
          loadClass("org.apache.spark.mllib.clustering.VectorWithNorm"),
          loadArrayClass("org.apache.spark.mllib.clustering.VectorWithNorm"),
          loadMatrixClass("org.apache.spark.mllib.clustering.VectorWithNorm"),
          loadClass("org.apache.spark.mllib.linalg.DenseVector"),
          loadArrayClass("org.apache.spark.mllib.linalg.DenseVector"),
          loadClass("org.apache.spark.mllib.linalg.SparseVector"),
          loadArrayClass("org.apache.spark.mllib.linalg.SparseVector"),

          loadClass("java.lang.Class"),
          loadClass("java.lang.Object"),
          loadArrayClass("java.lang.Object"),
          classOf[Array[Int]],
          classOf[Array[Long]],
          classOf[Array[Double]],
          classOf[Date]
      )

      val domainClasses = kryoDomainClasses.map(loadClass(_))

      val serializableTypes = reflections.getSubTypesOf(classOf[Serializable])
        .toArray.map(_.asInstanceOf[Class[_]])

      val types =
        serializableTypes
        .union(serializableTypes.map(clazz => loadArrayClass(clazz.getName)))
        .union(generalClasses)
        .union(domainClasses)
        .union(domainClasses.map(clazz => loadArrayClass(clazz.getName)))
        .distinct

      myConf.registerKryoClasses(types)
    }

    setConf(myConf)
    setSc(createSparkContext(myConf))
  }

  private def normPath(path: String) = {
    val filePrefix = s"file:${File.separatorChar}"
    val prefix = if (path.trim.startsWith(filePrefix)) filePrefix.substring(0, filePrefix.length - 1) else ""
    
    val otherSep = if ('/' == File.separatorChar) '\\' else '/'
    path.replace(otherSep, File.separatorChar).trim.substring(prefix.length)
  }
  
  private def concatToFile(opath: String, part: String) = {
    assert(!part.contains(File.separator))
    val dir = normPath(opath)

    if (dir.endsWith(File.separator)) dir + part else dir + File.separator + part
  }
  
  private def deleteRecur(path: String): Boolean = {
    deleteRecur(new File(path))
  }
  
  private def deleteRecur(path: File): Boolean = {
    val files = if (path.exists && path.isDirectory) path.listFiles else Array.empty[File]
    var allOk: Boolean = true

    for (f <- files) allOk &= deleteRecur(f)
    
    allOk & path.delete
  }
  
  private def createTempDir(dirName: String = "temp-dir") = {
    val tmp = File.createTempFile(dirName, "")
    deleteRecur(tmp)

    tmp.deleteOnExit
    tmp.getPath
  }

  private def createSparkContext(sparkConf: SparkConf) = {
    val mySc = new SparkContext(sparkConf)

    val sparkLocalDir = sparkConf.get("spark.local.dir", createTempDir("spark"))
    val checkpointDir = concatToFile(sparkLocalDir, "checkpoint")
    
    mySc.setCheckpointDir(checkpointDir)

    mySc
  }

  def conf = {
    if (sparkConf.isDefined) {
      sparkConf.get
    }
    else {
      sparkConf = Some(defaultSparkConf)
      sparkConf.get
    }
  }
  
  def sc = {
    if (sparkContext.isDefined) {
      sparkContext.get
    }
    else {
      sparkContext = Some(createSparkContext(conf))
      sparkContext.get
    }
  }
}
