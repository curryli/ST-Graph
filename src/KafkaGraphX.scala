import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
 
object KafkaGraphX {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  //设置运行环境
    val conf = new SparkConf().setAppName("GraphXStream") 
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint("checkpoint")
 
    conf.set("spark.streaming.unpersist", "false") 
    
    
    val vertexArray = Array(
      (0L, "")
    )
    
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(0L, 0L, 0)
    )
 
    //构造vertexRDD和edgeRDD
    var vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)
    var edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    val Array(zkQuorum, group, topics, numThreads) = args

    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //lines.print()
    
    var edgeInfo = lines.map(_.split("\\001"))
 
    var incEdgeDStream = edgeInfo.transform(rdd=>{
      rdd.map(x=>Edge( BKDRHash(x(0)), BKDRHash(x(1)), x(3).toInt))
    })
    

    
   val resultDstream = edgeInfo.transform{rdd=>
     var incErdd = rdd.map(x=>Edge( BKDRHash(x(0)), BKDRHash(x(1)), x(3).toInt))
     val unionERDD = edgeRDD.union(incErdd)
      edgeRDD = unionERDD 
      edgeRDD.cache()     //或者persist。       如果没有这句， 过段时间会出现   Attempted to use BlockRDD[69] at createStream at ... after its blocks have been removed
 
      var incVsrcrdd = rdd.map(x=>(BKDRHash(x(0)), x(0)))
      var incVdstrdd = rdd.map(x=>(BKDRHash(x(1)), x(1)))
      
     val unionVRDD = vertexRDD.union(incVsrcrdd).union(incVdstrdd)
      vertexRDD = unionVRDD
      vertexRDD.cache()     //或者persist。       如果没有这句， 过段时间会出现   Attempted to use BlockRDD[69] at createStream at ... after its blocks have been removed
      
      val graph = Graph(vertexRDD,edgeRDD)
      
      val vertice = graph.vertices   //随便什么，也可以是graph.degrees
      val edge = graph.edges   //随便什么，也可以是graph.degrees
      graph.unpersistVertices(blocking=false)
      //vertice
      edge
  }

    resultDstream.print()
  
    ssc.start()
    ssc.awaitTermination()
  }
  
  
  
  def BKDRHash( str:String) :Long ={
   val seed:Long  = 131 // 31 131 1313 13131 131313 etc..
   var hash:Long  = 0
   for(i <- 0 to str.length-1){
    hash = hash * seed + str.charAt(i)
    hash = hash.&("137438953471".toLong)        //0x1FFFFFFFFF              //固定一下长度
   }
   return hash 
}
  
}


