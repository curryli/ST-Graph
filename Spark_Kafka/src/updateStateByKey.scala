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
 
object updataStateByKey {
  def main(args: Array[String]) {
     //设置运行环境
    val conf = new SparkConf().setAppName("GraphXStream") 
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint("checkpoint")
 
    conf.set("spark.streaming.unpersist", "false") 
 
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val cardCount = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    
    val moneySum = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0)
      currentCount + previousCount
    }
    
    val moneySumDouble = (values: Seq[Double], state: Option[Double]) => {
      val currentCount = values.sum
      val previousCount = state.getOrElse(0.0)
      Some(currentCount + previousCount)
    }
    
    val moneySumTwo = (values: Seq[(Int,Int)], state: Option[(Int,Int)]) => {
      val sum1 = values.map(x=>x._1).sum
      val sum2 = values.map(x=>x._2).sum
      val previousCount = state.getOrElse((0,0))
      Some(previousCount._1 + sum1 , previousCount._2 + sum2)
    }
    
    
//    val moneySumList = (values: Seq[List[Int]], state: Seq[List[Int]]) => {        
//      val currentCount = values
//      //val previousCount = state
//      Some(currentCount.seq)
//    }
//    如果想用list可以尝试先对wordCounts进行transform{rdd=> rdd.combine{}} 转换为 list[Int]
    
 
    
    
    val Array(zkQuorum, group, topics, numThreads) = args
    
  // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    //lines.print()
    
    var edgeInfo = lines.map(_.split("\\001"))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")
 
    // 获取从Socket发送过来数据

    var words = lines.map(_.split("\\001"))
    
//  val wordCounts = words.map(x => (BKDRHash(x(0)), x(2).toInt))
//  val stateDstream = wordCounts.updateStateByKey[Int](moneySum)
    
//  val wordCounts = words.map(x => (BKDRHash(x(0)), x(2).toDouble))
//  val stateDstream = wordCounts.updateStateByKey[Double](moneySumDouble)
    
    val wordCounts = words.map(x => (BKDRHash(x(0)), (x(2).toInt,x(3).toInt)))
    val stateDstream = wordCounts.updateStateByKey[(Int,Int)](moneySumTwo)
    
    
    stateDstream.print()
    
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