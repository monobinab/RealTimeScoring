package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.SweepsTagCreatorBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class SweepsTopology {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SweepsTopology.class);

	public static void main(String[] args) {		
		
		if (!TopologyConfig.setEnvironment(args)) {
			System.out.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String kafkaTopic = TopicConstants.SWEEPS_KAFKA_TOPIC;
		String cps_sweeps_kafkaTopic = TopicConstants.RTS_CPS_SWEEPS_KAFKA_TOPIC;
		String zkroot = "sweeps_zkroot";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder builder = new TopologyBuilder();		
		
		try {
			builder.setSpout("RTSKafkaSpout", new RTSKafkaSpout(new KafkaUtil(System.getProperty(MongoNameConstants.IS_PROD)).getSpoutConfig(kafkaTopic, zkroot)), 1);
		} catch (ConfigurationException e1) {
			LOGGER.error(e1.getClass() + ": " + e1.getMessage(), e1);
			LOGGER.error("Kafka Not Initialised ");
			   System.exit(0);
		}	
		builder.setBolt("sweepTagCreatorBolt", new SweepsTagCreatorBolt(env), 1).shuffleGrouping("RTSKafkaSpout");
		builder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD), cps_sweeps_kafkaTopic), 1).shuffleGrouping("sweepTagCreatorBolt","kafka_stream");	
		Config conf = TopologyConfig.prepareStormConf("SWEEPS");
		conf.setMessageTimeoutSecs(7200);
		TopologyConfig.submitStorm(conf, builder, args[0]);
	}
}

