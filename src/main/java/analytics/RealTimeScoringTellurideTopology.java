package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.SpoutConfig;
import analytics.bolt.LoggingBolt;
import analytics.bolt.PurchaseScoreKafkaBolt;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TellurideParsingBoltPOS;
import analytics.bolt.TopologyConfig;
import analytics.spout.RTSKafkaSpout;
import analytics.spout.WebsphereMQSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.KafkaUtil;
import analytics.util.MQConnectionConfig;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class RealTimeScoringTellurideTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RealTimeScoringTellurideTopology.class);
	
	
	public static void main(String[] args) throws ConfigurationException {
		LOGGER.info("Starting telluride real time scoring topology");
		// Configure logger
		if (!TopologyConfig.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String blackoutKafkaTopic="rts_telluride_batch_blackout";
		String zkroot_telluride ="telluride_zkroot";
		String group_id = "telluride_groupid";
		String purchase_Topic="rts_cp_purchase_scores";
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		try {
			SpoutConfig spoutConfig = null;
			spoutConfig = new KafkaUtil(env).getSpoutConfig(blackoutKafkaTopic, zkroot_telluride, group_id);
			topologyBuilder.setSpout("tellurideKafkaSpout", new RTSKafkaSpout(spoutConfig), 1);
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage() +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			System.exit(0);	
		}
		
		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential(System.getProperty(MongoNameConstants.IS_PROD), "Telluride");
		if(mqCredential==null){
			LOGGER.error("Unable to get a MQ connections");
			return;
		}
		topologyBuilder
				.setSpout(
						"telluride1",
						new WebsphereMQSpout(mqCredential.getHostOneName(),
								mqCredential.getPort(), mqCredential
										.getQueueOneManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 3);
		topologyBuilder
				.setSpout(
						"telluride2",
						new WebsphereMQSpout(mqCredential.getHostTwoName(),
								mqCredential.getPort(), mqCredential
										.getQueueTwoManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 3);
		
		/*
		 * The following block of commented code is needs to be here, in case if we want data from kafka itself rather than MQ
		 */
		//BrokerHosts hosts = new ZkHosts("trprtelpacmapp1.vm.itg.corp.us.shldcorp.com:2181");
		// use topology Id as part of the consumer ID to make it unique
		/*SpoutConfig kafkaConfig = new SpoutConfig(hosts, "telprod_reqresp_log_output", "", "RTSConsumer_Telluride"+topologyId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		*/
		/*topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), 1);
		topologyBuilder.setBolt("kafkaParsingBolt", new TellurideKafkaParsingBoltPOS(System.getProperty(MongoNameConstants.IS_PROD),AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST),new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 12).shuffleGrouping("kafkaSpout");*/
		

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsingBolt", new TellurideParsingBoltPOS(System.getProperty(MongoNameConstants.IS_PROD),AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST),new Integer (AuthPropertiesReader
						.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 12).shuffleGrouping("telluride1").shuffleGrouping("telluride2").shuffleGrouping("tellurideKafkaSpout");
		
		//.shuffleGrouping("telluride1").shuffleGrouping("telluride2")

       topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader
				.getProperty(Constants.TELLURIDE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.TELLURIDE_REDIS_SERVER_PORT))), 12).shuffleGrouping("parsingBolt");
       
       topologyBuilder.setBolt("purchaseScoreKafka_bolt", new PurchaseScoreKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD), purchase_Topic), 2)
		.shuffleGrouping("strategyScoringBolt","cp_purchase_scores_stream");
              
       topologyBuilder.setBolt("kafka_bolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD),kafkatopic), 2).shuffleGrouping("strategyScoringBolt","kafka_stream");
	
       if(System.getProperty(MongoNameConstants.IS_PROD).equalsIgnoreCase("PROD")){
        	topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
       }
       
       Config conf = TopologyConfig.prepareStormConf("Telluride");
       conf.setNumWorkers(48);
       conf.setMessageTimeoutSecs(7200);
       TopologyConfig.submitStorm(conf, topologyBuilder, args[0]);
       
       /*Config conf = new Config();
		conf.put("metrics_topology", "Telluride");
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		conf.setDebug(false);
		
		if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {
			try {
                conf.setNumAckers(5);
                conf.setMessageTimeoutSecs(300);
                conf.setStatsSampleRate(1.0);
                conf.setNumWorkers(48);
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("telluride_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			}
			cluster.shutdown();
		}*/
	}
}

