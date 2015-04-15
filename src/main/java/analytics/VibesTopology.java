package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.VibesBolt;
import analytics.spout.VibesSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.SystemUtility;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class VibesTopology{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(VibesTopology.class);
	
	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting Vibes topology");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {
		
		TopologyBuilder builder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers("LOCAL");
		
		//Spout that wakes up every 5 mins and process the Traits
		builder.setSpout("vibesSpout", new VibesSpout(System.getProperty(MongoNameConstants.IS_PROD)), 1);
		builder.setBolt("vibesBolt",new VibesBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1)
				.shuffleGrouping("vibesSpout");

		Config conf = new Config();
			conf.put("metrics_topology", "PurchaseOccasion");
			//stormconf is set with system's property as MetricsListener needs it
			conf.put("topology_environment", System.getProperty(MongoNameConstants.IS_PROD));
			conf.registerMetricsConsumer(MetricsListener.class, 3);
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
				cluster.submitTopology("occassion_topology", conf,
						builder.createTopology());
				Thread.sleep(10000000);
				cluster.shutdown();
			}
		}

	}
}
	


