package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import analytics.bolt.ParsingBoltAAM_ATC;
import analytics.bolt.ParsingBoltAAM_InternalSearch;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;

public class AAM_InternalSearchTopology {

	static final Logger logger = LoggerFactory
			.getLogger(AAM_InternalSearchTopology.class);

	public static void main(String[] args) {
		String topic = TopicConstants.AAM_CDF_INTERNALSEARCH;
		int port = TopicConstants.PORT;

		RedisConnection redisConnection = new RedisConnection();
		String[] servers = redisConnection.getServers();

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("AAM_CDF_InternalSearch1",
				new AAMRedisPubSubSpout(servers[0], port, topic), 1);
		topologyBuilder.setSpout("AAM_CDF_InternalSearch2",
				new AAMRedisPubSubSpout(servers[1], port, topic), 1);
		topologyBuilder.setSpout("AAM_CDF_InternalSearch3",
				new AAMRedisPubSubSpout(servers[2], port, topic), 1);

		topologyBuilder
				.setBolt("ParsingBoltAAM_InternalSearch",
						new ParsingBoltAAM_InternalSearch())
				.shuffleGrouping("AAM_CDF_InternalSearch1")
				.shuffleGrouping("AAM_CDF_InternalSearch2")
				.shuffleGrouping("AAM_CDF_InternalSearch3");

		topologyBuilder.setBolt("strategy_bolt", new StrategyBolt(), 1)
				.shuffleGrouping("ParsingBoltAAM_InternalSearch");
		topologyBuilder.setBolt("scoring_bolt", new ScoringBolt(), 1)
				.shuffleGrouping("strategy_bolt");
		Config conf = new Config();

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				logger.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("aam_internal_search_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				logger.error(e.getClass() + ": " + e.getMessage(), e);
			}
			cluster.shutdown();

		}

	}
}
