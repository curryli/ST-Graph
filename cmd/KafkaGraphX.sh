/opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
--class KafkaGraphX \
--master local[8] \
GraphXStream.jar \
bb0103003:2181 test-consumer-group testgraphx 1
