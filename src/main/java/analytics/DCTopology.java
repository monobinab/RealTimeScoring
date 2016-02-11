package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.*;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltDC;
import analytics.bolt.PurchaseScoreKafkaBolt;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.DCTestSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import analytics.util.dao.caching.CacheRefreshScheduler;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;

public class DCTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParsingBoltDC.class);
	private static final int partition_num = 3;
	
	
	public static void main(String[] args) {
		
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String topologyId = "";
		String purchase_Topic= TopicConstants.PURCHASE_KAFKA_TOPIC;
		
		if (!TopologyConfig.setEnvironment(args)) {
			System.out.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		
		TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts("trprtelpacmapp1.vm.itg.corp.us.shldcorp.com:2181");
		
		// use topology Id as part of the consumer ID to make it unique
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, "telprod_reqresp_log_output", "", "RTSConsumer"+topologyId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), 2);
		
		/*DCTestSpout testSpout = new DCTestSpout();
		builder.setSpout("testSpout", testSpout, 2);*/
		/*builder.setBolt("dcParsingBolt", new ParsingBoltDC(System
		.getProperty(MongoNameConstants.IS_PROD)), 2).localOrShuffleGrouping("testSpout");*/
		
		builder.setBolt("dcParsingBolt", new ParsingBoltDC(System
				.getProperty(MongoNameConstants.ENV)), 2).localOrShuffleGrouping("kafkaSpout");
		
	    builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.ENV)),1).localOrShuffleGrouping("dcParsingBolt");
	    
	    builder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.ENV),kafkatopic), 1)
	    		.shuffleGrouping("strategyScoringBolt","kafka_stream");
	    
	    builder.setBolt("purchaseScoreKafka_bolt", new PurchaseScoreKafkaBolt(System.getProperty(MongoNameConstants.ENV), purchase_Topic), 2)
				.shuffleGrouping("strategyScoringBolt","cp_purchase_scores_stream");
		
	    if(System.getProperty(MongoNameConstants.ENV).equals("PROD")){
			builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.ENV)), 1)
					.shuffleGrouping("strategyScoringBolt", "score_stream");
		}	
		
		
		Config conf = TopologyConfig.prepareStormConf("DC");
		
		TopologyConfig.submitStorm(conf, builder, args[0]);
	}
}
