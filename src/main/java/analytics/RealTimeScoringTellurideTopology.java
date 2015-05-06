package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.LoggingBolt;
import analytics.bolt.ResponseBolt;
import analytics.bolt.ResponsysUnknownCallsBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TellurideParsingBoltPOS;
import analytics.spout.WebsphereMQSpout;
import analytics.util.AuthPropertiesReader;
import analytics.util.Constants;
import analytics.util.MQConnectionConfig;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;


/**
 * Created with IntelliJ IDEA. User: syermalk Date: 10/9/13 Time: 10:14 AM To
 * change this template use File | Settings | File Templates.
 */
public class RealTimeScoringTellurideTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RealTimeScoringTellurideTopology.class);
	
	
	public static void main(String[] args) throws ConfigurationException {
		LOGGER.info("Starting telluride real time scoring topology");
		// Configure logger
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();

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

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsingBolt", new TellurideParsingBoltPOS(System.getProperty(MongoNameConstants.IS_PROD)), 12).localOrShuffleGrouping("telluride1").localOrShuffleGrouping("telluride2");
        topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader
				.getProperty(Constants.TELLURIDE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.TELLURIDE_REDIS_SERVER_PORT))), 12).localOrShuffleGrouping("parsingBolt");
        if(System.getProperty(MongoNameConstants.IS_PROD).equalsIgnoreCase("PROD")){
        	topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
        }
       
        topologyBuilder.setBolt("responsysBolt", new ResponsysUnknownCallsBolt(System.getProperty(MongoNameConstants.IS_PROD), AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_HOST), new Integer (AuthPropertiesReader
				.getProperty(Constants.RESPONSE_REDIS_SERVER_PORT))), 3).shuffleGrouping("strategyScoringBolt", "response_stream");
		//Redis publish to server 1
        //topologyBuilder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 3).localOrShuffleGrouping("strategyScoringBolt", "score_stream");
        //topologyBuilder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 3).localOrShuffleGrouping("strategyScoringBolt", "member_stream");


		Config conf = new Config();
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
                conf.setNumWorkers(27);
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
		}
	}
}
