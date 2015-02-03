package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.FlumeRPCBolt;
import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.PersistBoostsBolt;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.SYWRedisSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class SYWEventsTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SYWEventsTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting syw events topology");
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers();
		String topic = TopicConstants.SYW;
/*
 * server1=rtsapp401p.prod.ch4.s.com
server2=rtsapp402p.prod.ch4.s.com
server3=rtsapp403p.prod.ch4.s.com
 */
		topologyBuilder.setSpout("sywEventsSpout1", new SYWRedisSpout(
				servers[0], TopicConstants.PORT, "SYW_Interactions"), 1);
		topologyBuilder.setSpout("sywEventsSpout2", new SYWRedisSpout(
				servers[1], TopicConstants.PORT, "SYW_Interactions"), 1);
		topologyBuilder.setSpout("sywEventsSpout3", new SYWRedisSpout(
				servers[2], TopicConstants.PORT, "SYW_Interactions"), 1);
		//rtsapp302p.qa.ch3.s.com
		//
		// Parse the JSON
		topologyBuilder.setBolt("parseEventsBolt", new ParsingBoltSYW(), 1)
				.shuffleGrouping("sywEventsSpout1").shuffleGrouping("sywEventsSpout2").shuffleGrouping("sywEventsSpout3");

		// Get the div line and boost variable
		topologyBuilder.setBolt("processSYWEvents",
				new ProcessSYWInteractions(), 4).shuffleGrouping(
				"parseEventsBolt");
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(), 1).shuffleGrouping("processSYWEvents", "score_stream");
		topologyBuilder.setBolt("persistBolt", new PersistBoostsBolt(), 1).shuffleGrouping("processSYWEvents", "persist_stream");
		topologyBuilder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		
		//topologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		//topologyBuilder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 2).shuffleGrouping("strategyScoringBolt", "member_stream");
		Config conf = new Config();
		conf.put("metrics_topology", "Syw");
	    conf.registerMetricsConsumer(MetricsListener.class, 3);
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args != null && args.length > 0) {
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
