package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ResponsysUnknownCallsBolt;
import analytics.spout.ResponsysSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class UnknownOccasionsTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(UnknownOccasionsTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting  unknownOccasions topology");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("unknownOccSpout", new ResponsysSpout(
				System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1);

		topologyBuilder.setBolt("responsysBolt", new ResponsysUnknownCallsBolt(System.getProperty(MongoNameConstants.IS_PROD)), 12).shuffleGrouping("unknownOccSpout");

			Config conf = new Config();
			conf.put("metrics_topology", "unknownOccasions");
			conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
			conf.setDebug(false);

			if (System.getProperty(MongoNameConstants.IS_PROD)
					.equalsIgnoreCase("PROD")
					|| System.getProperty(MongoNameConstants.IS_PROD)
							.equalsIgnoreCase("QA")) {
				try {
	               StormSubmitter.submitTopology(args[0], conf,
							topologyBuilder.createTopology());
				} catch (AlreadyAliveException e) {
					LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
				} catch (InvalidTopologyException e) {
					LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
				}
			} else {
				conf.setDebug(false);
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("unknownOccasions_topology", conf,
						topologyBuilder.createTopology());
				try {
					Thread.sleep(10000000);
				} catch (InterruptedException e) {
					LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
				}
				cluster.shutdown();
			}
	}
}