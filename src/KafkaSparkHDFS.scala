import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source
import scala.reflect.io.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Kafka Producer
  */
class ProduceSpark(sc: SparkContext) extends Runnable{

  private val props = new Properties()
  props.put("metadata.broker.list", "bB0104009" + ":" + "9092");
  props.put("serializer.class", "kafka.serializer.StringEncoder");


  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  
  def run() : Unit = {
    
    while(true){
      println("hello")
      val textfile = sc.textFile("xrli/AntiLD/HiveTrans03/*")

      try{
          textfile.foreach{line=>
            Thread.sleep(3000)
            val message = new KeyedMessage[String, String]("testgraphx", line);
            println(line)
            producer.send(message)
         }
  
      }catch{
        case e : Exception => println(e)
      }

      try{
        //sleep for 2 seconds after send a micro batch of message
        Thread.sleep(2000)
      }catch{
        case e : Exception => println(e)
      }
    }
  }

}


object KafkaSparkHDFS {
    def main(args : Array[String]): Unit ={
      //屏蔽日志
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
      val conf = new SparkConf().setAppName("GraphXStream") 
      val sc = new SparkContext(conf)
        
      

      new Thread(new ProduceSpark(sc: SparkContext)).start()
    }
}


///opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
//--class  KafkaSparkHDFS \
//--master local[8] \
//GraphXStream.jar \
//bB0104009:9092 testgraphx

