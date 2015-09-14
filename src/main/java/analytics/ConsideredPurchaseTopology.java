package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.SpoutConfig;
import analytics.bolt.CPParsePersistBolt;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.CPProcessingBolt;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class ConsideredPurchaseTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsideredPurchaseTopology.class);
	private static final int partition_num = 3;

	public static void main(String[] args) {		

		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		
		String kafkaTopic="rts_cp_membertags";
		String zkroot="rts_cp_Topic";
		//String kafkaTopic="stormtopic";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder topologyBuilder = new TopologyBuilder();		
				
		try {
			SpoutConfig spoutConfig = null;
			spoutConfig = new KafkaUtil(env).getSpoutConfig(kafkaTopic,zkroot);
			//spoutConfig.forceFromStart = true; //TODO - this needs to be removed.
			//spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
			topologyBuilder.setSpout("CPKafkaSpout", new RTSKafkaSpout(spoutConfig), 1);
			LOGGER.info("CPS Topology listening to kafka topic : " + kafkaTopic);
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			System.exit(0);	
		}	
		
		topologyBuilder.setBolt("CPParsePersistBolt", new CPParsePersistBolt(env), 1).shuffleGrouping("CPKafkaSpout");	
		topologyBuilder.setBolt("CPProcessingBolt", new CPProcessingBolt(env),1).localOrShuffleGrouping("CPParsePersistBolt");
		
		Config conf = new Config();
		conf.put("metrics_topology", "CPS");
		conf.registerMetricsConsumer(MetricsListener.class, env, partition_num);
		conf.setDebug(false);
		if (env.equalsIgnoreCase("PROD")|| env.equalsIgnoreCase("QA")) {	
			try {
				StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(partition_num);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("ConsideredPurchaseTopology", conf, topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();
		}
	
	}

}
