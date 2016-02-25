package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.BrowseCountPersistBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltAAM_InternalSearch;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.WebHDFSSpout;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class AAM_InternalSearchTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAM_InternalSearchTopology.class);

public static void main(String[] args) {
		
	if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String[] servers = RedisConnection.getServers(System.getProperty(MongoNameConstants.IS_PROD));
		String source = TopicConstants.AAM_CDF_INTERNALSEARCH;
		String browseKafkaTopic = TopicConstants.BROWSE_KAFKA_TOPIC;
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		//Sree. Spout that wakes up every 5 mins and process the Traits
		topologyBuilder.setSpout("internalSearchSpout", new WebHDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_INTERNAL_SEARCH_PATH, "aamInternalSearch"), 1);
		topologyBuilder.setBolt("ParsingBoltAAM_InternalSearch",new ParsingBoltAAM_InternalSearch(System.getProperty(MongoNameConstants.IS_PROD), source),10).shuffleGrouping("internalSearchSpout").setNumTasks(20).setDebug(true);
		topologyBuilder.setBolt("browseCountPersist", new BrowseCountPersistBolt(System.getProperty(MongoNameConstants.IS_PROD), source, "Browse", browseKafkaTopic), 3).shuffleGrouping("ParsingBoltAAM_InternalSearch", "browse_tag_stream");
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(System.getProperty(MongoNameConstants.IS_PROD)), 10)
				.shuffleGrouping("ParsingBoltAAM_InternalSearch");
		
		topologyBuilder.setBolt("kafka_bolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD),kafkatopic), 10)
		.shuffleGrouping("strategy_bolt","kafka_stream");
		
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategy_bolt", "score_stream");
		}
		
		
		Config conf = TopologyConfig.prepareStormConf("InternalSearch");
		conf.setMaxSpoutPending(30);
		TopologyConfig.submitStorm(conf, topologyBuilder, args[0]);
		
		/*Config conf = new Config();
		conf.put("metrics_topology", "InternalSearch");
		conf.setDebug(true);
		
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {	
			try {
				conf.setNumWorkers(6);
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

			}*/
		}
	}

