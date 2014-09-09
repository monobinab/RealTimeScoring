package analytics;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltAAM_ATC;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AAM_ATCTopology {

	static final Logger logger = LoggerFactory
			.getLogger(AAM_ATCTopology.class);

	public static void main(String[] args) throws ConfigurationException {

		logger.info("Starting web feed topology from ATC source");
		String topic = TopicConstants.AAM_ATC_PRODUCTS;
		int port = TopicConstants.PORT;

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		RedisConnection redisConnection = new RedisConnection();
		String[] servers = redisConnection.getServers();

		for (String server : servers) {
			topologyBuilder.setSpout(topic + server, new AAMRedisPubSubSpout(
					server, port, topic), 1);
		}

		BoltDeclarer boltDeclarer = topologyBuilder.setBolt(
				"ParsingBoltAAM_ATC", new ParsingBoltAAM_ATC(topic), 1);
		topologyBuilder.setBolt("strategy_bolt", new StrategyBolt(), 1)
				.shuffleGrouping("ParsingBoltAAM_ATC");
		topologyBuilder.setBolt("scoring_bolt", new ScoringBolt(), 1)
				.shuffleGrouping("strategy_bolt");

		for (String server : servers) {
			boltDeclarer.fieldsGrouping(topic + server, new Fields("uuid"));
		}

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
			cluster.submitTopology("aam_atc_topology", conf,
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
