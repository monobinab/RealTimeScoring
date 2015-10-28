package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.CPProcessingBolt;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.TECProcessingBolt;
import analytics.bolt.TagProcessingBolt;
import analytics.spout.RTSKafkaSpout;
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
import backtype.storm.topology.TopologyBuilder;

public class TagBuilderTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TagBuilderTopology.class);
	private static final int partition_num = 3;

	//indicates dev/qa/or prod
	public static void main(String[] args) {
		
	
		String topologyId = "";
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String kafkaTopic = TopicConstants.BROWSE_KAFKA_TOPIC;
		String zkroot="browseTopic";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder builder = new TopologyBuilder();		
				
		//prepare the kafka spout configuration	
	
		try {
			builder.setSpout(
					"RTSKafkaSpout",
					new RTSKafkaSpout(new KafkaUtil(System.getProperty(MongoNameConstants.IS_PROD)).getSpoutConfig(
							kafkaTopic,zkroot)), 1);
		} catch (ConfigurationException e1) {
			LOGGER.error(e1.getClass() + ": " + e1.getMessage(), e1);
			LOGGER.error("Kafka Not Initialised ");
			   System.exit(0);
			}	
		
		builder.setBolt("tagProcessingBolt", new TagProcessingBolt(env),10).localOrShuffleGrouping("RTSKafkaSpout");
		builder.setBolt("CPProcessingBolt", new CPProcessingBolt(env),10).shuffleGrouping("tagProcessingBolt");

		
		Config conf = new Config();
		conf.put("metrics_topology", "TB");
		conf.setMessageTimeoutSecs(7200);	
		conf.registerMetricsConsumer(MetricsListener.class, env, partition_num);
		conf.setDebug(false);
		if (env.equalsIgnoreCase("PROD")|| env.equalsIgnoreCase("QA")) {	
			try {
				conf.setNumWorkers(6);
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
