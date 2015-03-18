package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.FlumeRPCBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ParsingBoltAAM_ATC;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.spout.OccassionRedisSpout;
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
		/*System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}*/
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		String topic = TopicConstants.AAM_BROWSE_PRODUCTS;
		//int port = TopicConstants.PORT;

		//RedisConnection redisConnection = new RedisConnection();
		//String[] servers = RedisConnection.getServers();
		//int counter = 0;
		topologyBuilder.setSpout(topic + 0, new OccassionRedisSpout(
				0, topic), 1);
		topologyBuilder.setSpout(topic + 1, new OccassionRedisSpout(
				1, topic), 1);
		topologyBuilder.setSpout(topic + 2, new OccassionRedisSpout(
				2, topic), 1);
		
		topologyBuilder.setBolt("parsingBoltBrowse", new ParsingBoltOccassion(), 1)
		.shuffleGrouping(topic + 0).shuffleGrouping(topic +1 ).shuffleGrouping(topic + 2);
				
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(), 3)
				.localOrShuffleGrouping("parsingBoltBrowse");
		topologyBuilder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		
//		 topologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 3).localOrShuffleGrouping("strategyScoringBolt", "score_stream");
	//        topologyBuilder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 3).localOrShuffleGrouping("strategyScoringBolt", "member_stream");

		Config conf = new Config();
		conf.put("metrics_topology", "Product_Browse");
		conf.registerMetricsConsumer(MetricsListener.class, 3);
//		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
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
