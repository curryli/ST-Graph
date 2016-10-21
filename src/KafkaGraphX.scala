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
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    //边的数据类型ED:Int
    val edgeArray = Array(
      Edge(2L, 1L, 7)
    )
 
    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    var edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
 
    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    
    
    val Array(zkQuorum, group, topics, numThreads) = args


    
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //lines.print()
    
    val edgeInfo = lines.map(_.split("\\s+"))
 
    var incEdgeDStream = edgeInfo.transform(rdd=>{
      rdd.map(x=>Edge(x(0).toLong,x(1).toLong,x(2).toInt))
    })
    
    
    
   val newtemp = incEdgeDStream.transform{rdd=>
      val temp = edgeRDD.union(rdd)
      edgeRDD = temp
      edgeRDD.cache()     //或者persist。       如果没有这句， 过段时间会出现   Attempted to use BlockRDD[69] at createStream at ... after its blocks have been removed
      val graph = Graph.fromEdges(edgeRDD, "1")
      val result = graph.degrees
      graph.unpersistVertices(blocking=false)
      result
      
  }
    
    
    newtemp.print()

  
    ssc.start()
    ssc.awaitTermination()
  }
}


