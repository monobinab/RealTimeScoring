package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltAAM_InternalSearch;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.WebHDFSSpout;
import analytics.util.Constants;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.SystemUtility;
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
		
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		
		//RedisConnection redisConnection = new RedisConnection();
		String[] servers = RedisConnection.getServers(System.getProperty(MongoNameConstants.IS_PROD));

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		//Sree. Spout that wakes up every 5 mins and process the Traits
		topologyBuilder.setSpout("internalSearchSpout", new WebHDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_INTERNAL_SEARCH_PATH, "aamInternalSearch"), 1);
		topologyBuilder.setBolt("ParsingBoltAAM_InternalSearch",new ParsingBoltAAM_InternalSearch(System.getProperty(MongoNameConstants.IS_PROD), "aamInternalSearch"),3).shuffleGrouping("internalSearchSpout");

		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(System.getProperty(MongoNameConstants.IS_PROD)), 3)
				.shuffleGrouping("ParsingBoltAAM_InternalSearch");
		Config conf = new Config();
		conf.put("metrics_topology", "InternalSearch");
		
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {	
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

