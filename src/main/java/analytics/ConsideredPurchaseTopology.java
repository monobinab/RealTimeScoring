package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.SpoutConfig;
import analytics.bolt.CPParsePersistBolt;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.CPProcessingBolt;
import analytics.bolt.TagCreatorBolt;
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
			LOGGER.error("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		
		String kafkaTopic1="rts_cp_membertags";
		String zkroot1="rts_cp_membertags_zkroot";		
	//	String kafkaTopic2="stormtopic";
		String kafkaTopic2="rts_cp_membertags_qa";
		String zkroot2="rts_cp_stormtopic_zkroot";
		String cpsPurchaseScoresTopic="rts_cp_purchase_scores";
		String zkroot_cp_purchase = "rts_cp_purchase_zkroot";
		String group_id = "cps_groupid";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder topologyBuilder = new TopologyBuilder();		
				
		try {
			SpoutConfig spoutConfig1 = null;
			SpoutConfig spoutConfig2 = null;
			SpoutConfig spoutConfig3 = null;
			spoutConfig1 = new KafkaUtil(env).getSpoutConfig(kafkaTopic1,zkroot1,group_id);
			spoutConfig2 = new KafkaUtil(env).getSpoutConfig(kafkaTopic2,zkroot2,group_id);
			spoutConfig3 = new KafkaUtil(env).getSpoutConfig(cpsPurchaseScoresTopic,zkroot_cp_purchase,group_id);			
			topologyBuilder.setSpout("CPKafkaSpout1", new RTSKafkaSpout(spoutConfig1), 1);
			topologyBuilder.setSpout("CPKafkaSpout2", new RTSKafkaSpout(spoutConfig2), 1);
			spoutConfig2 = new KafkaUtil(env).getSpoutConfig(cpsPurchaseScoresTopic,zkroot_cp_purchase);
			topologyBuilder.setSpout("CPPurchaseFeedbackSpout", new RTSKafkaSpout(spoutConfig3), 1);
			LOGGER.info("CPS Topology listening to kafka topics : " + kafkaTopic1 + ", "+kafkaTopic2 +" , "+ cpsPurchaseScoresTopic);
			//LOGGER.info("CPS Topology listening to kafka topics : " + kafkaTopic1 + ", "+ cpsPurchaseScoresTopic);
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage() +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			System.exit(0);	
		}
		//topologyBuilder.setBolt("CPParsePersistBolt", new CPParsePersistBolt(env), 15).shuffleGrouping("CPKafkaSpout1").shuffleGrouping("CPTagCreatorBolt", "rtsTags_stream" );	
		topologyBuilder.setBolt("CPParsePersistBolt", new CPParsePersistBolt(env), 15).shuffleGrouping("CPKafkaSpout1").shuffleGrouping("CPKafkaSpout2").shuffleGrouping("CPTagCreatorBolt", "rtsTags_stream" );	
		topologyBuilder.setBolt("CPProcessingBolt", new CPProcessingBolt(env),15).shuffleGrouping("CPParsePersistBolt");
		topologyBuilder.setBolt("CPTagCreatorBolt", new TagCreatorBolt(env), 1).shuffleGrouping("CPPurchaseFeedbackSpout");		
		
		Config conf = new Config();
		conf.put("metrics_topology", "CPS");
		conf.registerMetricsConsumer(MetricsListener.class, env, partition_num);
		conf.setDebug(false);
		if (env.equalsIgnoreCase("PROD")|| env.equalsIgnoreCase("QA")) {	
			try {
				StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": "+ ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) +"  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + ExceptionUtils.getMessage(e) + "Rootcause-"+ ExceptionUtils.getRootCauseMessage(e) +"  STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
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
