package analytics;

import org.apache.commons.configuration.ConfigurationException;

import analytics.bolt.StrategyScoringBolt;
import analytics.spout.TestingSpout;
import analytics.spout.WebHDFSSpout;
import analytics.util.Constants;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TestingTopology {
	
	public static void main(String[] args) throws ConfigurationException {
	TopologyBuilder builder = new TopologyBuilder();
	builder.setSpout("testSpout", new TestingSpout(), 1);
	System.setProperty("rtseprod", "LOCAL");
	builder.setBolt("strategyBolt", new StrategyScoringBolt("PROD"), 1).shuffleGrouping("testSpout");
	
	
	Config conf = new Config();
	conf.setDebug(false);
	conf.setNumWorkers(3);
	if (System.getProperty(MongoNameConstants.IS_PROD)
			.equalsIgnoreCase("PROD")
			|| System.getProperty(MongoNameConstants.IS_PROD)
					.equalsIgnoreCase("QA")) {
		try {
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			//LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
		} catch (InvalidTopologyException e) {
			//LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
		}
	} else {
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("AAMTraitsTopology", conf,
				builder.createTopology());
		try {
			Thread.sleep(10000000);
		} catch (InterruptedException e) {
			//LOGGER.debug("Unable to wait for topology", e);
		}
		cluster.shutdown();

	}

	}
}
