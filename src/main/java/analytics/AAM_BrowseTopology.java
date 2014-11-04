package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ParsingBoltAAM_ATC;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
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

public class AAM_BrowseTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAM_BrowseTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Starting web feed topology from browse source");
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		String topic = TopicConstants.AAM_BROWSE_PRODUCTS;
		int port = TopicConstants.PORT;

		//RedisConnection redisConnection = new RedisConnection();
		String[] servers = RedisConnection.getServers();
		int counter = 0;
		for (String server : servers) {
			topologyBuilder.setSpout(topic + ++counter, new AAMRedisPubSubSpout(
					server, port, topic), 1);
		}

		BoltDeclarer boltDeclarer = topologyBuilder.setBolt(
				"ParsingBoltAAM_ATC", new ParsingBoltAAM_ATC(topic), 3);
		topologyBuilder.setBolt("strategy_scoring_bolt", new StrategyScoringBolt(), 3)
				.localOrShuffleGrouping("ParsingBoltAAM_ATC");
		 topologyBuilder.setBolt("score_publish_bolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 3).localOrShuffleGrouping("strategy_scoring_bolt", "score_stream");
	        topologyBuilder.setBolt("member_publish_bolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 3).localOrShuffleGrouping("strategy_scoring_bolt", "member_stream");


		for (String server : servers) {
			boltDeclarer.fieldsGrouping(topic + server, new Fields("uuid"));
		}

		Config conf = new Config();
		conf.put("metrics_topology", "Telluride");
		conf.registerMetricsConsumer(MetricsListener.class, 3);
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("aam_browse_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
			cluster.shutdown();

		}

	}
}
