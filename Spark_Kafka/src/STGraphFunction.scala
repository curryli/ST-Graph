import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import org.apache.kafka.clients.producer._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
 
object STGraphFunction {
  def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }
  
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  //设置运行环境
    val conf = new SparkConf().setAppName("GraphXStream") 
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))   //修改每次处理时间的间隔
    //ssc.checkpoint("checkpoint")
 
    conf.set("spark.streaming.unpersist", "true") 
    
    
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
      edgeRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)     //或者persist。       如果没有这句， 过段时间会出现   Attempted to use BlockRDD[69] at createStream at ... after its blocks have been removed
 
      var incVsrcrdd = rdd.map(x=>(BKDRHash(x(0)), x(0)))
      var incVdstrdd = rdd.map(x=>(BKDRHash(x(1)), x(1)))
      
      val unionVRDD = vertexRDD.union(incVsrcrdd).union(incVdstrdd)
      vertexRDD = unionVRDD
      vertexRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)    //或者persist。       如果没有这句， 过段时间会出现   Attempted to use BlockRDD[69] at createStream at ... after its blocks have been removed
      
      val graph = Graph(vertexRDD,edgeRDD)
      
      //Degrees操作
 
    
    
    val vcount = graph.numVertices 
    val ecount = graph.numEdges
    
    val MAXOUT = graph.outDegrees.reduce(max) 
    val MAXIN = graph.inDegrees.reduce(max) 
    val MAXDeg =  graph.degrees.reduce(max)
    //println
    
    graph.unpersistVertices(blocking=false)
    

    sc.parallelize(Seq(vcount, ecount, MAXOUT, MAXIN, MAXDeg))   //sc.parallelize(List(MAXOUT, MAXIN, MAXDeg))
       
    //如果想用checkpoint，必须要把 Seq(vcount, ecount, MAXOUT, MAXIN, MAXDeg) 自定义一个类结构，然后extends　serializable　进行序列化
     
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


//KafkaGraphxX.sh
//KafkaProducerHDFS.sh

