package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.PersistOccasionBolt;
import analytics.bolt.RTSInterceptorBolt;
import analytics.bolt.ResponseBolt;
import analytics.spout.OccassionRedisSpout;
import analytics.spout.RTSKafkaSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.KafkaUtil;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class PurchaseOccassionTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PurchaseOccassionTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting purchase occassion topology");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			String topic = TopicConstants.OCCASSION;
			topologyBuilder.setSpout(
					"occassionSpout1",
					new OccassionRedisSpout(0, topic, System
							.getProperty(MongoNameConstants.IS_PROD)), 1);
			topologyBuilder.setSpout(
					"occassionSpout2",
					new OccassionRedisSpout(1, topic, System
							.getProperty(MongoNameConstants.IS_PROD)), 1);
			topologyBuilder.setSpout(
					"occassionSpout3",
					new OccassionRedisSpout(2, topic, System
							.getProperty(MongoNameConstants.IS_PROD)), 1);
			
			//Newly added code for adding kafka to the topology
			String kafkaTopic="rts_cp_membertags";

			topologyBuilder.setSpout(
					"RTSKafkaSpout",
					new RTSKafkaSpout(KafkaUtil.getSpoutConfig(
							
							System.getProperty(MongoNameConstants.IS_PROD),
							kafkaTopic)), 1);	
			
			
			topologyBuilder.setBolt("RTSInterceptorBolt", new RTSInterceptorBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1)
			.shuffleGrouping("RTSKafkaSpout");			
			
			topologyBuilder.setBolt("parseOccassionBolt", new ParsingBoltOccassion(System.getProperty(MongoNameConstants.IS_PROD),
					AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
					.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 3)
			.shuffleGrouping("occassionSpout1").shuffleGrouping("occassionSpout2").shuffleGrouping("occassionSpout3").shuffleGrouping("RTSInterceptorBolt");

			
			topologyBuilder.setBolt(
					"persistOccasionBolt",
					new PersistOccasionBolt(System
							.getProperty(MongoNameConstants.IS_PROD)), 3)
					.shuffleGrouping("parseOccassionBolt");
			/*topologyBuilder.setBolt(
					"strategy_bolt",
					new StrategyScoringBolt(System
							.getProperty(MongoNameConstants.IS_PROD)), 3)
					.shuffleGrouping("persistOccasionBolt");*/

		//Sree. Added the new bolt for Responses
		topologyBuilder.setBolt("responses_bolt", new ResponseBolt(System
				.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 48)
		//.shuffleGrouping("strategy_bolt", "response_stream")
		.shuffleGrouping("persistOccasionBolt", "response_stream_from_persist");
		
			Config conf = new Config();
			conf.put("metrics_topology", "PurchaseOccasion");
			//Added the timeout so that topology will not read the message again
			conf.setMessageTimeoutSecs(86400);	
			//stormconf is set with system's property as MetricsListener needs it
			conf.put("topology_environment", System.getProperty(MongoNameConstants.IS_PROD));
			conf.registerMetricsConsumer(MetricsListener.class,  System.getProperty(MongoNameConstants.IS_PROD), 3);
			//conf.setMaxSpoutPending(500);
			
			if (System.getProperty(MongoNameConstants.IS_PROD)
					.equalsIgnoreCase("PROD")
					|| System.getProperty(MongoNameConstants.IS_PROD)
							.equalsIgnoreCase("QA")) {
				conf.setNumWorkers(6);
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} else {
				conf.setDebug(false);
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("occassion_topology", conf,
						topologyBuilder.createTopology());
				Thread.sleep(10000000);
				cluster.shutdown();
			}
		}
	}
}
