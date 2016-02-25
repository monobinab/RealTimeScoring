package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.BrowseCountPersistBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltAAM_Browse;
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

public class AAM_BrowseTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAM_BrowseTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Starting web feed topology from browse source");
		
		if (!TopologyConfig.setEnvironment(args)) {
			System.out.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String topic = TopicConstants.AAM_BROWSE_PRODUCTS;
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String browseKafkaTopic = TopicConstants.BROWSE_KAFKA_TOPIC;
		//RedisConnection redisConnection = new RedisConnection();
		String[] servers = RedisConnection.getServers(System.getProperty(MongoNameConstants.IS_PROD));
		
		//Sree. Spout that wakes up every 5 mins and process the Traits
		topologyBuilder.setSpout("browseSpout", new WebHDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_BROWSER_PATH, "aamBrowser"), 1);
		topologyBuilder.setBolt("parsingBoltBrowse", new ParsingBoltAAM_Browse(System.getProperty(MongoNameConstants.IS_PROD), topic), 3).shuffleGrouping("browseSpout");
		topologyBuilder.setBolt("browseCountPersist", new BrowseCountPersistBolt(System.getProperty(MongoNameConstants.IS_PROD), topic, "Browse", browseKafkaTopic), 3).shuffleGrouping("parsingBoltBrowse", "browse_tag_stream");

		
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 3)
				.localOrShuffleGrouping("parsingBoltBrowse");
		
		topologyBuilder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD),kafkatopic), 1)
			.shuffleGrouping("strategyScoringBolt","kafka_stream");
		
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			//topologyBuilder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
			topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}
		
//		 topologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 3).localOrShuffleGrouping("strategyScoringBolt", "score_stream");
	//        topologyBuilder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 3).localOrShuffleGrouping("strategyScoringBolt", "member_stream");

		Config conf = TopologyConfig.prepareStormConf("Product_Browse");
		conf.setMaxSpoutPending(30);
		TopologyConfig.submitStorm(conf, topologyBuilder, args[0]);
		
		/*Config conf = new Config();
		conf.put("metrics_topology", "Product_Browse");
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		conf.setMaxSpoutPending(30);
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
			cluster.submitTopology("aam_browse_topology", conf,
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
