package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.TECProcessingBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class TECTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(TECTopology.class);
	private static final int partition_num = 3;

	//indicates dev/qa/or prod
	public static void main(String[] args) {
		
	
		String topologyId = "";
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String kafkaTopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String zkroot="tecTopic";
		String group_id = "tec_groupid";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder builder = new TopologyBuilder();		
				
		//prepare the kafka spout configuration	
	
		try {
			builder.setSpout(
					"RTSKafkaSpout",
					new RTSKafkaSpout(new KafkaUtil(System.getProperty(MongoNameConstants.IS_PROD)).getSpoutConfig(
							kafkaTopic,zkroot,group_id)), 1);
			
		} catch (ConfigurationException e1) {
			LOGGER.error(e1.getClass() + ": " + e1.getMessage(), e1);
			LOGGER.error("Kafka Not Initialised ");
			   System.exit(0);
			}	
		
		
		builder.setBolt("tecProcessingBolt", new TECProcessingBolt(env),2).localOrShuffleGrouping("RTSKafkaSpout");
		
		Config conf = TopologyConfig.prepareStormConf("TEC");
		conf.setMessageTimeoutSecs(7200);
		TopologyConfig.submitStorm(conf, builder, args[0]);
		
		/*Config conf = new Config();
		conf.put("metrics_topology", "TEC");
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
		}*/
	}
}
