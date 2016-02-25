package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingSignalBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.SignalSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class SignalTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Starting SignalTopology");
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("signalSpout", new SignalSpout(), 1);
		topologyBuilder.setBolt("parsingSignalBolt", new ParsingSignalBolt(System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 3).shuffleGrouping("signalSpout");

		
		Config conf = TopologyConfig.prepareStormConf("Signal_topology");
		conf.setMaxSpoutPending(30);
		TopologyConfig.submitStorm(conf, topologyBuilder, args[0]);
		
		/*Config conf = new Config();
		conf.put("metrics_topology", "Signal_topology");
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		conf.setMaxSpoutPending(30);
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
			cluster.submitTopology("signal_topology", conf,
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