/opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
--class  KafkaProducer \
--master local[8] \
GraphXStream.jar \
bB0104009:9092 testlxr
