package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.EmailFeedbackParsingBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.PurchaseScoreKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TopologyConfig;
import analytics.spout.RTSKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class EmailFeedbackTopology {

	private static final Logger LOGGER = LoggerFactory.getLogger(EmailFeedbackTopology.class);
	private static final int partition_num = 3;
	
	public static void main(String[] args) {
		
		String purchase_Topic="rts_cp_purchase_scores";
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String kafkaTopic = "rts_emailnofeedback";
		String zkroot="emailTopic";
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder builder = new TopologyBuilder();
		
		//browseKafkaTopic for PROD
		String browseKafkaTopic = "rts_browse";
		
		//for testing
		//String browseKafkaTopic = "test2";
		
		//listening topic for testing
		/*String kafkaTopic = "test2";*/
		
		//prepare the kafka spout configuration			
		try {
			builder.setSpout("RTSKafkaSpout", new RTSKafkaSpout(new KafkaUtil(System.getProperty(MongoNameConstants.IS_PROD)).getSpoutConfig(				
								kafkaTopic,zkroot)), 1);
		} catch (ConfigurationException e1) {				
				LOGGER.error(e1.getClass() + ": " + e1.getMessage(), e1);
				LOGGER.error("Kafka Not Initialised ");
				System.exit(0);
		}
			
		//builder.setBolt("emailFeedbackParsingBolt", new EmailFeedbackParsingBolt(env),2).localOrShuffleGrouping("RTSKafkaSpout");
		builder.setBolt("emailFeedbackParsingBolt", new EmailFeedbackParsingBolt(env, "Browse", browseKafkaTopic),2).localOrShuffleGrouping("RTSKafkaSpout");
		builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(env), 2).shuffleGrouping("emailFeedbackParsingBolt",  "score_stream");
		
		//Adding the purchase kafka bolt to read from strategy scoring bolt...
		builder.setBolt("purchaseScoreKafka_bolt", new PurchaseScoreKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD), purchase_Topic), 2)
		.shuffleGrouping("strategyScoringBolt","cp_purchase_scores_stream");
		
		if(env.equals("PROD")){
			builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}
		
		
		Config conf = TopologyConfig.prepareStormConf("EF");
		conf.setNumWorkers(2);
		conf.setMessageTimeoutSecs(7200);
		TopologyConfig.submitStorm(conf, builder, args[0]);
		
		/*Config conf = new Config();
		conf.put("metrics_topology", "EF");
		conf.setMessageTimeoutSecs(7200);	
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), partition_num);
		conf.setDebug(false);
		if (System.getProperty(MongoNameConstants.IS_PROD).equalsIgnoreCase("PROD")|| System.getProperty(MongoNameConstants.IS_PROD).equalsIgnoreCase("QA")) {	
			try {
				conf.setNumWorkers(6);
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(true);
			conf.setMaxTaskParallelism(partition_num);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("EmailFeedbackTopology", conf, builder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();
		}*/		
	}
}
