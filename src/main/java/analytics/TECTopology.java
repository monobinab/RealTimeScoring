package analytics;

import java.util.UUID;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import analytics.bolt.LoggingBolt;
import analytics.bolt.TECProcessingBolt;
import analytics.util.KafkaUtil;
//import analytics.spout.RTSKafkaSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class TECTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TECTopology.class);
	private static final int partition_num = 3;

	//indicates dev/qa/or prod
	public static void main(String[] args) {
		
	
		String topologyId = "";
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String topic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder builder = new TopologyBuilder();		
				
		//prepare the kafka spout configuration	
		try {
			builder.setSpout("KafkaSpout", new KafkaSpout(KafkaUtil.getSpoutConfig(System.getProperty(MongoNameConstants.IS_PROD),topic)), 1);
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			System.exit(0);			
		}
		
		builder.setBolt("tecProcessingBolt", new TECProcessingBolt(env),2).localOrShuffleGrouping("KafkaSpout");
		
		Config conf = new Config();
		conf.put("metrics_topology", "TEC");
		conf.registerMetricsConsumer(MetricsListener.class, env, partition_num);
		conf.setDebug(false);
		if (env.equalsIgnoreCase("PROD")|| env.equalsIgnoreCase("QA")) {	
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(partition_num);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("TECTopology", conf, builder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();
		}
	}
}
