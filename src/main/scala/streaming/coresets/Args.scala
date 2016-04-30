package streaming.coresets

import scala.collection.mutable.HashMap

object Args {
  case class Params(
    verbose: Boolean = false,
    
    denseData: Boolean = false,
  
    localRDDs: Boolean = false,
    
    generateProfile: Boolean = false,
  
    alg: String = "",
  
    algParams: String = "",
    
    sampleSize: Int = -1,
    
    batchSecs: Int = 10,
    
    parallelism: Option[Int] = None,
  
    input: String = "",
    
    output: String = "",
    
    checkpointDir: String = "",
    
    mode: String = "",
  
    sparkParams: Map[String, String] = Map(
  //    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
  //    "spark.kryo.registrationRequired" -> "false"
  //    "spark.kryoserializer.buffer.max.mb" -> "1024"
    )
  )
  
  private def mergeMaps(l: Map[String, String], r: Map[String, String]) = {
    val res = new HashMap[String, String]
    l.foreach(res += _)
    r.foreach(t => if (!res.contains(t._1)) res += t)
    res.toMap
  }

  def cli(args: Array[String]): Params = {
    val parser = new scopt.OptionParser[Params]("WCS Indexer") {
      head("Coreset tool", "1.0")
      
      opt[Unit]('v', "verbose") action {
        (_, c) => c.copy(verbose = true)
      } text ("verbose is a flag")
      
      opt[Unit]('d', "denseData") action {
        (_, c) => c.copy(denseData = true)
      } text ("vector data is dense rather than sparse")
      
      opt[Unit]('l', "localRDDs") action {
        (_, c) => c.copy(localRDDs = true)
      } text ("do all sampling locally in driver")
      
      opt[Unit]('p', "generateProfile") action {
        (_, c) => c.copy(generateProfile = true)
      } text ("generate runtime profile information about sampling time (works only in local[])")
      
      opt[String]('a', "algorithm") required () action {
        (x, c) => c.copy(alg = x)
      } text ("supported algorithms are spark-kmeans, coreset-kmeans, coreset-svd")
      
      opt[String]("algorithmParams") required () action {
        (x, c) => c.copy(algParams = x)
      } text ("send paramaters to algorithm")
  
      opt[Int]("sampleSize") optional () action {
        (x, c) => c.copy(sampleSize = x)
      } text("sample size for coresets")
      
      opt[Int]("batchSecs") optional () action {
        (x, c) => c.copy(batchSecs = x)
      } text("mini batch size in seconds (for streaming)")
      
      opt[Int]("parallelism") optional () action {
        (x, c) => c.copy(parallelism = Some(x))
      } text("parallelism for effecting repartitioning")

      opt[String]('c', "checkpointDir") optional () action {
        (x, c) => c.copy(checkpointDir = x)
      } text("the checkpointDir for spark, can be on FS or HDFS")

      opt[String]('i', "input") required () action {
        (x, c) => c.copy(input = x)
      } text("input file or source")
      
      opt[String]('o', "output") required () action {
        (x, c) => c.copy(output = x)
      } text("the suffix of the path to use for output centers RDD")
      
      opt[String]('m', "mode") required () action {
        (x, c) => c.copy(mode = x)
      } text("mode can be 'bulk', 'streaming' or 'evaluate'")
      
      opt[Map[String, String]]("sparkParams") valueName ("k1=v1, k2=v2, ...") action {
        (x, c) => c.copy(sparkParams = mergeMaps(x, c.sparkParams))
      } text ("these are parameters to pass on for the spark configuration")

      help("help") text ("for help contact royl@il.ibm.com")
    }

    val ores = parser.parse(args.toSeq, Params())
    
    if (ores.isEmpty) {
      throw new RuntimeException("bad input paramaters")
    }
    
    ores.get
  }
}
