package kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Kafka612TesterOLD {

	public static void main(String[] args) {		
           Properties props = new Properties();
           //String kafkaserver= args[0];
           //String kafkaport =args[1];  
          
           /* uncomment this 
           //Passed parameters hfdvsywanakaf1.vm.itg.corp.us.shldcorp.com    9092    
           kafkaserver="trprsywanakaf1.vm.itg.corp.us.shldcorp.com";
           //kafkaserver="hfdvsywanakaf1.vm.itg.corp.us.shldcorp.com";
           kafkaport="9092"; 
                      props.put("metadata.broker.list", kafkaserver+":"+kafkaport);
           */
           
           
          // String kafkaURL="hfdvsywanakaf1.vm.itg.corp.us.shldcorp.com:9092";
          String kafkaURL="trprsywanakaf1.vm.itg.corp.us.shldcorp.com:9092,trprsywanakaf2.vm.itg.corp.us.shldcorp.com:9092,trprsywanakaf3.vm.itg.corp.us.shldcorp.com:9092";
           
           //9092
           props.put("metadata.broker.list", kafkaURL);
	       props.put("serializer.class", "kafka.serializer.StringEncoder");
	       //props.put("partitioner.class", "kafka.TestPartitioner");
	       //props.put("request.required.acks", "1");
	 	   ProducerConfig config = new ProducerConfig(props);
	 	   Producer<String, String> producer = new Producer<String, String>(config);
           //String msg = "New Test message to Kafka at "+System.currentTimeMillis(); 
	 	   //String msg="{\"lyl_id_no\":\"7081327008588950\",\"tags\":[\"HMMTS823600153010\",\"HAVCS823600153010\",\"HALAS823600153010\",\"HARFS723600123010\", \"HAGAS118400116010\"]}";
	 	   String msg="{\"lyl_id_no\":\"7081073754330425\",\"BU\":\"HA\",\"format\":\"S\",\"emailFeedbackResponse\":\"YES\"}";
           KeyedMessage<String, String> data = new KeyedMessage<String, String>("rts_emailnofeedback", "rts_grp", msg);
	       producer.send(data);
           producer.close();       
	        
           //Consumer
           
           
           
	    }

}
