package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.FacebookBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.FacebookRedisSpout;
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

		LOGGER.info("Starting social topology ");
		String topic = TopicConstants.FB;
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers();

		topologyBuilder.setSpout("facebookSpout", new FacebookRedisSpout(
				servers[0], TopicConstants.PORT, topic), 1);
		topologyBuilder.setBolt("facebookBolt", new FacebookBolt())
				.shuffleGrouping("facebookSpout");
		topologyBuilder.setBolt("strategyBolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("facebookBolt");
		Config conf = new Config();

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
