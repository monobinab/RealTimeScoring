package analytics;

import org.apache.commons.configuration.ConfigurationException;

import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltDC;
import analytics.bolt.PurchaseScoreKafkaBolt;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.SpoutConfig;

public class DCTopology {

	public static void main(String[] args) {
		
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String purchase_Topic= TopicConstants.PURCHASE_KAFKA_TOPIC;
		
		if (!TopologyConfig.setEnvironment(args)) {
			System.out.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		String dcKafkaTopic="telprod_reqresp_log_output";
		String zkroot_dc="dc_zkroot_new";
		String group_id = "RTSConsumer";
		SpoutConfig spoutConfig = null;
		TopologyBuilder builder = new TopologyBuilder();
		try {
			spoutConfig = new KafkaUtil(env, "dc").getDCSpoutConfig(dcKafkaTopic, zkroot_dc, group_id);
			//spoutConfig = new KafkaUtil(env, "dc").getDCSpoutConfig(dcKafkaTopic, group_id);
			builder.setSpout("dcKafkaSpout", new RTSKafkaSpout(spoutConfig), 1);
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
			
		/*DCTestSpout testSpout = new DCTestSpout();
		builder.setSpout("testSpout", testSpout, 2);*/
		/*builder.setBolt("dcParsingBolt", new ParsingBoltDC(System
		.getProperty(MongoNameConstants.IS_PROD)), 2).localOrShuffleGrouping("testSpout");*/
		
		builder.setBolt("dcParsingBolt", new ParsingBoltDC(System
				.getProperty(MongoNameConstants.IS_PROD)), 2).localOrShuffleGrouping("dcKafkaSpout");
		
	    builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)),1).localOrShuffleGrouping("dcParsingBolt");
	    
	    builder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD),kafkatopic), 1)
	    		.shuffleGrouping("strategyScoringBolt","kafka_stream");
	    
	    builder.setBolt("purchaseScoreKafka_bolt", new PurchaseScoreKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD), purchase_Topic), 2)
				.shuffleGrouping("strategyScoringBolt","cp_purchase_scores_stream");
		
	    if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1)
					.shuffleGrouping("strategyScoringBolt", "score_stream");
		}	
		
		
		Config conf = TopologyConfig.prepareStormConf("DC");
		conf.setMessageTimeoutSecs(7200);
		TopologyConfig.submitStorm(conf, builder, args[0]);
	}
}
