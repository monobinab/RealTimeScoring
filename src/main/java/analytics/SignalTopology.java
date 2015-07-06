package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.SignalBolt;
import analytics.spout.SignalSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class SignalTopology {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SignalTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("Starting SignalRedisTopology");
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} 
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("signalRedisSpout", new SignalSpout());
		topologyBuilder.setBolt("signalRedisBolt", new SignalBolt(System.getProperty(MongoNameConstants.IS_PROD), "10.2.8.175", 11211), 3).shuffleGrouping("signalRedisSpout");

		Config conf = new Config();
		conf.put("metrics_topology", "Signal_Redis");
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
			cluster.submitTopology("signal_redis_topology", conf,
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