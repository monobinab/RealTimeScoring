package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.POSPurchaseBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.ResponsysSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class POSPurchaseTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(POSPurchaseTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting  POS Purchase topology");
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("posPurchaseSpout", new ResponsysSpout(
				System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 1);

		topologyBuilder.setBolt("responsysBolt", new POSPurchaseBolt(System.getProperty(MongoNameConstants.IS_PROD)), 12).shuffleGrouping("posPurchaseSpout");

			
		Config conf = TopologyConfig.prepareStormConf("posPurchase");
		
		TopologyConfig.submitStorm(conf, topologyBuilder, args[0]);
		conf.setNumWorkers(3);
		/*Config conf = new Config();
			conf.put("metrics_topology", "posPurchase");
			conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
			conf.setDebug(false);

			if (System.getProperty(MongoNameConstants.IS_PROD)
					.equalsIgnoreCase("PROD")
					|| System.getProperty(MongoNameConstants.IS_PROD)
							.equalsIgnoreCase("QA")) {
				try {
					conf.setNumWorkers(6);
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
				cluster.submitTopology("posPurchase_topology", conf,
						topologyBuilder.createTopology());
				try {
					Thread.sleep(10000000);
				} catch (InterruptedException e) {
					LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
				}
				cluster.shutdown();
			}*/
	}
}