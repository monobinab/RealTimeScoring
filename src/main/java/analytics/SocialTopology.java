package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.SocialBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.spout.FacebookRedisSpout;
import analytics.spout.TwitterRedisSpout;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class SocialTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SocialTopology.class);

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, InterruptedException {
		/*System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}*/
		LOGGER.info("Starting social topology ");
		String facebookTopic = TopicConstants.FB;
		String twitterTopic = TopicConstants.TW;
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		//String[] servers = RedisConnection.getServers();

		topologyBuilder.setSpout("facebookSpout1", new FacebookRedisSpout(
				0, facebookTopic), 1);
		topologyBuilder.setSpout("facebookSpout2", new FacebookRedisSpout(
				1, facebookTopic), 1);
		topologyBuilder.setSpout("facebookSpout3", new FacebookRedisSpout(
				2, facebookTopic), 1);
		topologyBuilder.setSpout("twitterSpout1", new TwitterRedisSpout(
				0, twitterTopic), 1);
		topologyBuilder.setSpout("twitterSpout2", new TwitterRedisSpout(
				1, twitterTopic), 1);
		topologyBuilder.setSpout("twitterSpout3", new TwitterRedisSpout(
				2, twitterTopic), 1);
		topologyBuilder.setBolt("socialBolt", new SocialBolt())
				.shuffleGrouping("facebookSpout1")
				.shuffleGrouping("facebookSpout2")
				.shuffleGrouping("facebookSpout3")
				.shuffleGrouping("twitterSpout1")
				.shuffleGrouping("twitterSpout2")
				.shuffleGrouping("twitterSpout3");
		topologyBuilder.setBolt("strategyBolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("socialBolt");
		Config conf = new Config();
	//	conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					topologyBuilder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("social_topology", conf,
					topologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
