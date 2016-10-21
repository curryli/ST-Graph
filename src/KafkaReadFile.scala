import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source
import scala.reflect.io.Path


/**
  * Kafka Producer
  */
class KafkaProduceMsg() extends Runnable{

  private val props = new Properties()
  props.put("metadata.broker.list", "bB0104009" + ":" + "9092");
  props.put("serializer.class", "kafka.serializer.StringEncoder");


  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)

  
  def run() : Unit = {
    while(true){
      val file = "/home/hdanaly/xrli/graphx/GStream/graphTest.txt"

      try{
        
          val reader = Source.fromFile(file.toString(), "UTF-8")

          for(line <- reader.getLines()){
            Thread.sleep(3000)
            val message = new KeyedMessage[String, String]("testlxr", null, line);
            println(line);
            producer.send(message)
          }

  
      }catch{
        case e : Exception => println(e)
      }

      try{
        //sleep for 3 seconds after send a micro batch of message
        Thread.sleep(3000)
      }catch{
        case e : Exception => println(e)
      }
    }
  }

}


//scp transaction root@bB0104009:/usr/tmp/          bB0104009随便找个文件也行

object ProduceMsg {
    def main(args : Array[String]): Unit ={
      new Thread(new KafkaProduceMsg()).start()
    }
}