package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.SpoutConfig;
import analytics.bolt.CPParsePersistBolt;
import analytics.bolt.CPProcessingBolt;
import analytics.bolt.PurchaseBolt;
import analytics.bolt.TagCreatorBolt;
import analytics.bolt.TagProcessingBolt;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;
import analytics.util.dao.caching.CacheRefreshScheduler;
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
		
		/* USE WHEN TESTING
		String mdTagsKafkaTopic="rts_cp_membertags_qa";
		String zkroot_mdtags="rts_cp_membertags_qa_zkroot";
		String cpsPurchaseScoresTopic="rts_cp_purchase_scores_qa";
		String zkroot_cp_purchase = "purchase_scores_qa_zkroot";
		//Browse related changes...
		String browseKafkaTopic = "rts_browse_qa";
		String zkroot_browse="browseTopic_qa_zkroot"; */
		
		// USE FOR PRODUCTION
		String mdTagsKafkaTopic="cps_rtstags_qa";
		String zkroot_mdtags="cps_rtstags_qa_zkroot";
		String cpsPurchaseScoresTopic="rts_cp_purchase_scores";
		String zkroot_cp_purchase = "rts_cp_purchase_zkroot";		
		//Browse related changes...
		String browseKafkaTopic = TopicConstants.BROWSE_KAFKA_TOPIC;
		String zkroot_browse="browseTopic";
		
		String group_id = "cps_groupid";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder topologyBuilder = new TopologyBuilder();	
		CacheRefreshScheduler.getInstance().startScheduler();
		
		try {
			SpoutConfig spoutConfig1 = null;
			SpoutConfig spoutConfig2 = null;
			spoutConfig1 = new KafkaUtil(env).getSpoutConfig(mdTagsKafkaTopic,zkroot_mdtags,group_id);
			spoutConfig2 = new KafkaUtil(env).getSpoutConfig(cpsPurchaseScoresTopic,zkroot_cp_purchase,group_id);	
			
		
			topologyBuilder.setSpout("MDTagsSpout", new RTSKafkaSpout(spoutConfig1), 1);
			topologyBuilder.setSpout("CPPurchaseSpout", new RTSKafkaSpout(spoutConfig2), 1);
			
			//Browse related changes
			topologyBuilder.setSpout(
					"BrowseKafkaSpout",
					new RTSKafkaSpout(new KafkaUtil(env).getSpoutConfig(browseKafkaTopic,zkroot_browse,group_id)), 1);			
			
			LOGGER.info("CPS Topology listening to kafka topics : " + mdTagsKafkaTopic + ", "+ cpsPurchaseScoresTopic);
			
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage() +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			System.exit(0);	
		}
		topologyBuilder.setBolt("CPTagCreatorBolt", new TagCreatorBolt(env), 1).shuffleGrouping("CPPurchaseSpout");	
		topologyBuilder.setBolt("CPParsePersistBolt", new CPParsePersistBolt(env), 5).shuffleGrouping("MDTagsSpout").shuffleGrouping("CPTagCreatorBolt", "rtsTags_stream" );	
					
		//Browse Related Changes
		topologyBuilder.setBolt("tagProcessingBolt", new TagProcessingBolt(env),5).localOrShuffleGrouping("BrowseKafkaSpout");
		topologyBuilder.setBolt("CPProcessingBolt", new CPProcessingBolt(env),10).shuffleGrouping("CPParsePersistBolt").shuffleGrouping("tagProcessingBolt")
		.shuffleGrouping("CPTagCreatorBolt", "blackedout_stream" );
		
		Config conf = new Config();
		conf.put("metrics_topology", "CPS");
		//Added the timeout so that topology will not read the message again
		conf.setMessageTimeoutSecs(86400);	
		conf.put("topology_environment", System.getProperty(MongoNameConstants.IS_PROD));
		conf.registerMetricsConsumer(MetricsListener.class, env, partition_num);
		
		if (env.equalsIgnoreCase("PROD")|| env.equalsIgnoreCase("QA")) {	
			try {
				conf.setNumWorkers(6);
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
