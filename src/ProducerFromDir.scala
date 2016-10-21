import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source
import scala.reflect.io.Path


/**
  * Kafka Producer
  */
class ProduceDir(brokerList : String, topic : String) extends Runnable{

  private val BROKER_LIST = brokerList //"master:9092,worker1:9092,worker2:9092"
  private val TARGET_TOPIC = topic //"new"
  private val DIR = "/home/hdanaly/xrli/graphx/GStream/"
  
  private val props = new Properties()
  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.StringEncoder")


  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  
  def run() : Unit = {
    while(true){
      val files = Path(this.DIR).walkFilter(p => p.isFile && p.name.contains("graphTest.txt"))

      try{
         for(file <- files){
          val reader = Source.fromFile(file.toString(), "UTF-8")

          for(line <- reader.getLines()){
            Thread.sleep(3000)
            val message = new KeyedMessage[String, String](this.TARGET_TOPIC, line)
            println(line)
            producer.send(message)
          }
          
           //produce完成后，将文件copy到另一个目录，之后delete
//          val fileName = file.toFile.name
//          file.toFile.copyTo(Path("/root/Documents/completed/" +fileName + ".completed"))
//          file.delete()
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


object KafkaProducer {
    def main(args : Array[String]): Unit ={
      new Thread(new ProduceDir(args(0),args(1))).start()
    }
}


///opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
//--class  KafkaProducer \
//--master local[8] \
//GraphXStream.jar \
//bB0104009:9092 testlxr

