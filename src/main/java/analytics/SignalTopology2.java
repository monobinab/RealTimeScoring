package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltAAM_Browse;
import analytics.bolt.SignalBolt2;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.SignalSpout2;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class SignalTopology2{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalTopology2.class);
	
	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting Signal topology 2");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {
		
		TopologyBuilder builder = new TopologyBuilder();
		String topic = TopicConstants.SIGNAL_FEED;
		
		//Spout that wakes up every 3 mins and process the Vibes Text Messages
		builder.setSpout("2_signalSpout", new SignalSpout2(System.getProperty(MongoNameConstants.IS_PROD),
				AuthPropertiesReader.getProperty(Constants.TELLURIDE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1);
		
		builder.setBolt("2_signalBolt",new SignalBolt2(System.getProperty(MongoNameConstants.IS_PROD)), 3)
				.shuffleGrouping("2_signalSpout");
		
		builder.setBolt("parsingBoltBrowse", new ParsingBoltAAM_Browse(System.getProperty(MongoNameConstants.IS_PROD), topic), 3).shuffleGrouping("2_signalBolt");

		
		builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 3)
				.localOrShuffleGrouping("parsingBoltBrowse");
		
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			//topologyBuilder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
			builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}

		Config conf = new Config();
			conf.put("metrics_topology", "2_Signal");
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
				cluster.submitTopology("2_signal_topology", conf,
						builder.createTopology());
				Thread.sleep(10000000);
				cluster.shutdown();
			}
		}

	}
}
	


