package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.PersistBoostsBolt;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.SYWRedisSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

  /*This topology demonstrates Storm's stream groupings and multilang
  capabilities.
 */
public class SYWEventsTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SYWEventsTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting syw events topology");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String topic = TopicConstants.SYW;
 
		topologyBuilder.setSpout("sywEventsSpout1", new SYWRedisSpout(
				0, topic, System
				.getProperty(MongoNameConstants.IS_PROD)), 1);
		topologyBuilder.setSpout("sywEventsSpout2", new SYWRedisSpout(
				1, topic, System
				.getProperty(MongoNameConstants.IS_PROD)), 1);
		topologyBuilder.setSpout("sywEventsSpout3", new SYWRedisSpout(
				2, topic, System
				.getProperty(MongoNameConstants.IS_PROD)), 1);
		
		// Parse the JSON
		topologyBuilder.setBolt("parseEventsBolt", new ParsingBoltSYW(System
				.getProperty(MongoNameConstants.IS_PROD)), 1)
				.shuffleGrouping("sywEventsSpout1").shuffleGrouping("sywEventsSpout2").shuffleGrouping("sywEventsSpout3");

		// Get the div line and boost variable
		topologyBuilder.setBolt("processSYWEvents",
				new ProcessSYWInteractions(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping(
				"parseEventsBolt");
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("processSYWEvents", "score_stream");
		topologyBuilder.setBolt("persistBolt", new PersistBoostsBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("processSYWEvents", "persist_stream");
		
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}	
		//topologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		//topologyBuilder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 2).shuffleGrouping("strategyScoringBolt", "member_stream");
		Config conf = new Config();
		conf.put("metrics_topology", "Syw");
	    conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
	
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
			cluster.submitTopology("syw_topology", conf,
					topologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
