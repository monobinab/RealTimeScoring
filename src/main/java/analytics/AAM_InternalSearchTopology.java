/*package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltAAM_InternalSearch;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.spout.WebHDFSSpout;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class AAM_InternalSearchTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAM_InternalSearchTopology.class);

	public static void main(String[] args) {
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		String topic = TopicConstants.AAM_CDF_INTERNALSEARCH;
		int port = TopicConstants.PORT;

		//RedisConnection redisConnection = new RedisConnection();
		String[] servers = RedisConnection.getServers();

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("AAM_CDF_InternalSearch1",
				new AAMRedisPubSubSpout(servers[0], port, topic), 1);
		topologyBuilder.setSpout("AAM_CDF_InternalSearch2",
				new AAMRedisPubSubSpout(servers[1], port, topic), 1);
		topologyBuilder.setSpout("AAM_CDF_InternalSearch3",
				new AAMRedisPubSubSpout(servers[2], port, topic), 1);
		
		//Sree. Spout that wakes up every 5 mins and process the Traits
		topologyBuilder.setSpout("internalSearchSpout", new WebHDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_INTERNAL_SEARCH_PATH, "aamInternalSearch"), 1);
		topologyBuilder.setBolt("ParsingBoltAAM_InternalSearch",new ParsingBoltAAM_InternalSearch()).shuffleGrouping("internalSearchSpout");
		
		topologyBuilder
				.setBolt("ParsingBoltAAM_InternalSearch",
						new ParsingBoltAAM_InternalSearch())
				.shuffleGrouping("AAM_CDF_InternalSearch1")
				.shuffleGrouping("AAM_CDF_InternalSearch2")
				.shuffleGrouping("AAM_CDF_InternalSearch3");

		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("ParsingBoltAAM_InternalSearch");
		Config conf = new Config();
	//	conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
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
			cluster.submitTopology("aam_internal_search_topology", conf,
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