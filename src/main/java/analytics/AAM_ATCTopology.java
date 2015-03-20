/*package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.FlumeRPCBolt;
import analytics.bolt.ParsingBoltAAM_ATC;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.spout.OccassionRedisSpout;
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

public class AAM_ATCTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAM_ATCTopology.class);

	public static void main(String[] args) throws ConfigurationException {

		LOGGER.info("Starting web feed topology from ATC source");
		//System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		String topic = TopicConstants.AAM_ATC_PRODUCTS;

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("atcSpout1", new OccassionRedisSpout(
				0, topic), 1);
		topologyBuilder.setSpout("atcSpout2", new OccassionRedisSpout(
				1, topic), 1);
		topologyBuilder.setSpout("atcSpout3", new OccassionRedisSpout(
				2, topic), 1);
		
		topologyBuilder.setBolt("parsingBoltBrowse", new ParsingBoltOccassion(), 1)
		.shuffleGrouping(topic + 0).shuffleGrouping(topic +1 ).shuffleGrouping(topic + 2);
				
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(), 3)
				.localOrShuffleGrouping("parsingBoltBrowse");
		topologyBuilder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		topologyBuilder.setBolt("ParsingBoltAAM_ATC", new ParsingBoltOccassion(), 1)
		.shuffleGrouping(topic + 0).shuffleGrouping(topic +1 ).shuffleGrouping(topic + 2);
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("ParsingBoltAAM_ATC");
		
		String[] servers = RedisConnection.getServers();

		for (String server : servers) {
			topologyBuilder.setSpout(topic + server, new AAMRedisPubSubSpout(
					server, TopicConstants.PORT, topic), 1);
		}*

		BoltDeclarer boltDeclarer = topologyBuilder.setBolt(
				"ParsingBoltAAM_ATC", new ParsingBoltAAM_ATC(topic), 1);
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("ParsingBoltAAM_ATC");
		int counter = 0;
		
		/*for (String server : servers) {
			topologyBuilder.setSpout(topic + ++counter, new AAMRedisPubSubSpout(
					server, TopicConstants.PORT, topic), 1);
			boltDeclarer.shuffleGrouping(topic + counter);
		}
		Config conf = new Config();
		//conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
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
			cluster.submitTopology("aam_atc_topology", conf,
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
*/