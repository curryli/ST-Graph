import java.io.BufferedReader;
import java.io.IOException;  
import java.io.InputStreamReader;
import java.net.URI;   
import java.util.Properties;  

import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  

public class KafkaProducerHDFS{
	
	private static final String TOPIC = "testgraphx"; //kafka创建的topic  

	public static void main(String[] args) throws IOException, InterruptedException {    
		    Properties props = new Properties();
		    props.put("metadata.broker.list", "bB0104009:9092");  //这两句要放在 ProducerConfig config = new ProducerConfig(props);  之前设置
			props.put("serializer.class", "kafka.serializer.StringEncoder"); 
			
	        ProducerConfig config = new ProducerConfig(props);  
	        Producer<String, String> producer = new Producer<String, String>(config);  
			
	
	        Configuration cfg = new Configuration();  
	        cfg.set("fs.defaultFS", "hdfs://nameservice1");
	        cfg.set("dfs.nameservices", "nameservice1");
	        cfg.set("dfs.ha.namenodes.nameservice1", "namenode1285,namenode1176");
	        cfg.set("dfs.client.failover.proxy.provider.nameservice1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
	        cfg.set("dfs.namenode.rpc-address.nameservice1.namenode1285", "bB0203002:8020");   ///etc/hadoop/conf    到hdfs-site.xml文件里找8020端口（默认的namenode RPC交互端口进行配置）  
	        cfg.set("dfs.namenode.rpc-address.nameservice1.namenode1176", "bB0103002:8020");  //有两个8020端口，都配置进去HDFS 的HA 功能通过配置Active/Standby 两个NameNodes 实现在集群中对NameNode 的热备

	        //String hdfs = "hdfs://nameservice1/user/hdanaly/xrli/hdfstest";   
	        String hdfs = "hdfs://nameservice1/user/hdanaly/xrli/HiveTrans03";  
	        
	        
	        FileSystem fs = FileSystem.get(URI.create(hdfs),cfg);  
		    FileStatus fileList[] = fs.listStatus(new Path(hdfs));  
		      
		      BufferedReader br = null;
		      for (FileStatus file : fileList) { 
		        FSDataInputStream hdfsInStream = fs.open(file.getPath());
		        br = new BufferedReader(new InputStreamReader(hdfsInStream));  
		    	  
		        String line = null;
	            while (null != (line = br.readLine())) {
	      	      Thread.sleep(5);
	      	      KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, null, line);
	      	      //System.out.println(line);
	              producer.send(message);
	      	  
	            }
		      }
	}    	  
	  
	}  


//java -cp GraphXStream.jar -Djava.ext.dirs=hdfsjar KafkaProducerHDFS  
//或者下面都可以

///opt/cloudera/parcels/CDH-5.6.0-1.cdh5.6.0.p0.45/lib/spark/bin/spark-submit \
//--class KafkaProducerHDFS \
//--master local[8] \
//GraphXStream.jar