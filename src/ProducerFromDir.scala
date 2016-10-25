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
      val files = Path(this.DIR).walkFilter(p => p.isFile && p.name.contains("001098_0"))

      try{
         for(file <- files){
          val reader = Source.fromFile(file.toString(), "UTF-8")

          for(line <- reader.getLines()){
            Thread.sleep(1000)
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
        Thread.sleep(1000)
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


//hadoop fs -cat xrlhadoop fs -get xrli/HiveTrans03/001098_0
//c1a92aef096e9c64cf3daee937d53b984cabca5e55944f67e6ae9bd2bca12352300000.01
//08ad95d3c09dd616905784ff932a03e44cb0326d6bc4471c5b071ae75a0f86b9150000.01

