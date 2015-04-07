package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.PersistOccasionBolt;
import analytics.bolt.ResponseBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.OccassionRedisSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.HostPortUtility;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;

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

			/*topologyBuilder.setBolt("parseOccassionBolt", new ParsingBoltOccassion(
					System.getProperty(MongoNameConstants.IS_PROD)),1)
					.shuffleGrouping("occassionSpout1")
					.shuffleGrouping("occassionSpout2")
					.shuffleGrouping("occassionSpout3");*/
			
			topologyBuilder.setBolt("parseOccassionBolt", new ParsingBoltOccassion(System.getProperty(MongoNameConstants.IS_PROD),
					AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
					.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1)
			.shuffleGrouping("occassionSpout1");//.shuffleGrouping("occassionSpout2").shuffleGrouping("occassionSpout3");

			
			topologyBuilder.setBolt(
					"persistOccasionBolt",
					new PersistOccasionBolt(System
							.getProperty(MongoNameConstants.IS_PROD)), 1)
					.shuffleGrouping("parseOccassionBolt", "persist_stream");
			topologyBuilder.setBolt(
					"strategy_bolt",
					new StrategyScoringBolt(System
							.getProperty(MongoNameConstants.IS_PROD)), 1)
					.shuffleGrouping("parseOccassionBolt");

		//Sree. Added the new bolt for Responses
		topologyBuilder.setBolt("responses_bolt", new ResponseBolt(System
				.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 2)
		.shuffleGrouping("strategy_bolt", "response_stream");
			Config conf = new Config();
			conf.put("metrics_topology", "PurchaseOccasion");
			//stormconf is set with system's property as MetricsListener needs it
			conf.put("topology_environment", System.getProperty(MongoNameConstants.IS_PROD));
			conf.registerMetricsConsumer(MetricsListener.class, 3);
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
