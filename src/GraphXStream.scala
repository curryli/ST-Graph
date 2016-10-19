import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
 
object GraphXStream {
  def main(args: Array[String]) {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    //设置运行环境
    val conf = new SparkConf().setAppName("GraphXStream") 
    val sc = new SparkContext(conf)
 
    val vertexList = List(
      (1L, "Alice"),
      (2L, "Bob"),
      (3L, "Charlie"),
      (4L, "David"),
      (5L, "Ed"),
      (6L, "Fran")
    )
    //边的数据类型ED:Int
    var edgeList = List(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2)
    )
    
    //构造vertexRDD和edgeRDD
    var vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexList)
    
    
    val ssc = new StreamingContext(sc, Seconds(2))
    
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    lines.print()
    
    val edgeInfo = lines.map(_.split("\\t"))
 
    val incEdgeDStream = edgeInfo.transform(rdd=>{
      rdd.map(x=>Edge(x(0).toLong,x(1).toLong,x(2).toInt))
    })
    
    incEdgeDStream.foreachRDD{ rdd => 
       edgeList = edgeList.:::(rdd.collect().toList)
         
       
       println(edgeList.length)
       
       edgeList.foreach(edge=>
          println("srcId: " + edge.srcId + " dstId: " + edge.dstId + " attr: " + edge.attr)
       )
      
   }
    

    ssc.start()
    ssc.awaitTermination()
  }
}




//
//object GraphXStream {
//  def main(args: Array[String]) {
//    //屏蔽日志
//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
// 
//    //设置运行环境
//    val conf = new SparkConf().setAppName("GraphXStream") 
//    val sc = new SparkContext(conf)
// 
//    val vertexArray = Array(
//      (1L, "Alice"),
//      (2L, "Bob"),
//      (3L, "Charlie"),
//      (4L, "David"),
//      (5L, "Ed"),
//      (6L, "Fran")
//    )
//    //边的数据类型ED:Int
//    var edgeArray = List(
//      (2L, 1L, 7),
//      (2L, 4L, 2)
//    )
//    
//    //构造vertexRDD和edgeRDD
//    var vertexRDD: RDD[(Long, String)] = sc.parallelize(vertexArray)
//    //var edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
//    
//    val ssc = new StreamingContext(sc, Seconds(2))
//    
//    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
//    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
//    lines.print()
//    
//    val edgeInfo = lines.map(_.split("\\t"))
// 
//    val incEdgeDStream = edgeInfo.transform(rdd=>{
//      rdd.map(x=>(x(0).toLong,x(1).toLong,x(2).toInt))
//    })
//    
//    incEdgeDStream.foreachRDD{ rdd => 
//       edgeArray = edgeArray.:::(rdd.collect().toList)
//         
//       edgeArray.foreach(edge=>
//          println("srcId: " + edge._1 + " dstId: " + edge._2 + " attr: " + edge._3)
//    )
// 
//   }
//    
//    
//    
//    //    val numEdges = sc.makeRDD(Seq(edgeList.length))
////    val edgeStreamRDD = ssc.queueStream(mutable.Queue.empty[RDD[Int]], oneAtATime = true, defaultRDD = numEdges)
////
////    var unionEdgeDStream = ssc.union(Seq(edgeStreamRDD))
////    unionEdgeDStream.foreachRDD(rdd=>
////       rdd.collect().foreach(println)
////      )
////      
//    
//    
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}