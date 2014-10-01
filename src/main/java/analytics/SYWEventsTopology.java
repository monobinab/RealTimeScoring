package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.PersistBoostsBolt;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.SywScoringBolt;
import analytics.spout.SYWRedisSpout;
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
		TopologyBuilder toplologyBuilder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers();
		String topic = TopicConstants.SYW;
/*
 * server1=rtsapp401p.prod.ch4.s.com
server2=rtsapp402p.prod.ch4.s.com
server3=rtsapp403p.prod.ch4.s.com
 */
		toplologyBuilder.setSpout("SYWEventsSpout", new SYWRedisSpout(
				"rtsapp401p.prod.ch4.s.com", TopicConstants.PORT, "SYW_Interactions"), 1);
		// Parse the JSON
		toplologyBuilder.setBolt("ParseEventsBolt", new ParsingBoltSYW(), 1)
				.shuffleGrouping("SYWEventsSpout");
		// Get the div line and boost variable
		toplologyBuilder.setBolt("ProcessSYWEvents",
				new ProcessSYWInteractions(), 4).shuffleGrouping(
				"ParseEventsBolt");
		toplologyBuilder.setBolt("scoringBolt", new SywScoringBolt(), 1)
				.shuffleGrouping("ProcessSYWEvents");
		//TODO: Persist is still being fixed
		toplologyBuilder.setBolt("persistBolt", new PersistBoostsBolt(), 1)
		.shuffleGrouping("ProcessSYWEvents");
		toplologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 1).shuffleGrouping("scoringBolt");
		toplologyBuilder.setBolt("member_publish_bolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 2).shuffleGrouping("scoringBolt", "member_stream");
		Config conf = new Config();
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args != null && args.length > 0) {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf,
					toplologyBuilder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("syw_topology", conf,
					toplologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
