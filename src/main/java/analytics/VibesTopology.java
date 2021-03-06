package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.TopologyConfig;
import analytics.bolt.VibesBolt;
import analytics.spout.VibesSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class VibesTopology{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VibesTopology.class);
	
	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting Vibes topology");
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {
		
		TopologyBuilder builder = new TopologyBuilder();
		//String[] servers = RedisConnection.getServers(System.getProperty(MongoNameConstants.IS_PROD));
		
		//Spout that wakes up every 5 mins and process the Vibes Text Messages
		builder.setSpout("vibesSpout", new VibesSpout(System.getProperty(MongoNameConstants.IS_PROD),
				AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1);
		
		builder.setBolt("vibesBolt",new VibesBolt(System.getProperty(MongoNameConstants.IS_PROD)), 3)
				.shuffleGrouping("vibesSpout");

		Config conf = TopologyConfig.prepareStormConf("VibesMetrics");
		conf.setNumWorkers(2);
		TopologyConfig.submitStorm(conf, builder, args[0]);
		
		/*Config conf = new Config();
			conf.put("metrics_topology", "VibesMetrics");
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
				cluster.submitTopology("vibes_topology", conf,
						builder.createTopology());
				Thread.sleep(10000000);
				cluster.shutdown();
			}*/
		}
	}
}
	


