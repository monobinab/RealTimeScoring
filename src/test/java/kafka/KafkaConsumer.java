package kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
 
 
/**
 * Created by user on 8/4/14.
 */
public class KafkaConsumer {
    
    private final String topic;
    private final ConsumerConnector consumerConnector;
  
    public KafkaConsumer(String zookeeper, String groupId, String topic)
    {
        Properties properties = new Properties();
        properties.put("zookeeper.connect",zookeeper); //<zookeeper_node:port>
        properties.put("group.id",groupId);
        properties.put("zookeeper.session.timeout.ms", "1000");
        properties.put("zookeeper.sync.time.ms", "500");
        properties.put("auto.commit.interval.ms", "2000");
       
        consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        this.topic = topic;
    }
 
    public void testConsumer() {
        Map<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumerConnector.createMessageStreams(topicCount);
        List<KafkaStream<byte[], byte[]>> streams =  consumerStreams.get(topic);
        
        for(final KafkaStream stream : streams){
        	 ConsumerIterator<byte[], byte[]> it = stream.iterator();
             while(it.hasNext())
                 System.out.println("message from Single Topic :: " + new String(it.next().message()));        	
        }
        if(consumerConnector != null)
        	consumerConnector.shutdown();
    }
      
    public static void main(String[] argv) {
    	//hfdvsywanazoo1.vm.itg.corp.us.shldcorp.com:2181/kafka rts_group test2 parameters in QA    	
    	//trprsywanazoo1.vm.itg.corp.us.shldcorp.com:2181/kafka rts_group test1
        KafkaConsumer helloKafkaConsumer = new KafkaConsumer(argv[0],argv[1],argv[2]);
        helloKafkaConsumer.testConsumer();
    } 
   
}
