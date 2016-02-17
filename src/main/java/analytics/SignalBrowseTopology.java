package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.BrowseCountPersistBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltAAM_Browse;
import analytics.bolt.ParsingSignalBrowseBolt;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.SignalBrowseSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class SignalBrowseTopology{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalBrowseTopology.class);
	
	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting Signal Browse topology ");
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {
		
		TopologyBuilder builder = new TopologyBuilder();
		String source = TopicConstants.SIGNAL_BROWSE_FEED;
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String browseKafkaTopic = TopicConstants.BROWSE_KAFKA_TOPIC;
		
		builder.setSpout("signalBrowseSpout", new SignalBrowseSpout(System.getProperty(MongoNameConstants.IS_PROD),
				AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1);
		/*builder.setSpout("signalBrowseSpout", new SignalBrowseSpout(System.getProperty(MongoNameConstants.IS_PROD),
				"10.2.8.149", 11211), 1);*/
		builder.setBolt("parsingSignalBrowseBolt",new ParsingSignalBrowseBolt(System.getProperty(MongoNameConstants.IS_PROD)), 3)
				.shuffleGrouping("signalBrowseSpout");
		builder.setBolt("parsingBoltBrowse", new ParsingBoltAAM_Browse(System.getProperty(MongoNameConstants.IS_PROD), source), 3).shuffleGrouping("parsingSignalBrowseBolt");
		builder.setBolt("browseCountPersist", new BrowseCountPersistBolt(System.getProperty(MongoNameConstants.IS_PROD), source, "Browse", browseKafkaTopic), 3).shuffleGrouping("parsingBoltBrowse", "browse_tag_stream");
		builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 3)
				.localOrShuffleGrouping("parsingBoltBrowse");
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}
		builder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD), kafkatopic), 1)
		.shuffleGrouping("strategyScoringBolt","kafka_stream");

		Config conf = TopologyConfig.prepareStormConf("SignalBrowse");
		
		TopologyConfig.submitStorm(conf, builder, args[0]);
		
		/*Config conf = new Config();
			conf.put("metrics_topology", "SignalBrowse");
			//stormconf is set with system's property as MetricsListener needs it
			conf.registerMetricsConsumer(MetricsListener.class,System.getProperty(MongoNameConstants.IS_PROD), 3);
			if (System.getProperty(MongoNameConstants.IS_PROD)
					.equalsIgnoreCase("PROD")
					|| System.getProperty(MongoNameConstants.IS_PROD)
							.equalsIgnoreCase("QA")) {
				conf.setNumWorkers(6);
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} else {
				conf.setDebug(false);
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("signal_browse_topology", conf,
						builder.createTopology());
				Thread.sleep(10000000);
				cluster.shutdown();
			}*/
		}
	}
}
	


