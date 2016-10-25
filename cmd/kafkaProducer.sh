/opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
--class KafkaWordCountProducer \
--master local[8] \
--jars kafka_2.10-0.8.2.0-kafka-1.3.0.jar,KAFKA-5.6.0.jar,kafka-clients-0.8.2.0-kafka-1.3.0.jar \
GraphXStream.jar \
bB0104009:9092 test_lxr 3 5
