package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.TellurideParsingBoltPOS;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
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
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential("Telluride");
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
										.getQueueName()), 2);
		topologyBuilder
				.setSpout(
						"telluride2",
						new WebsphereMQSpout(mqCredential.getHostTwoName(),
								mqCredential.getPort(), mqCredential
										.getQueueTwoManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 2);

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsing_bolt", new TellurideParsingBoltPOS(), 6)
				.shuffleGrouping("telluride1").shuffleGrouping("telluride2");
        topologyBuilder.setBolt("strategy_scoring_bolt", new StrategyScoringBolt(), 8).shuffleGrouping("parsing_bolt");
        //Redis publish to server 1



		Config conf = new Config();
		conf.registerMetricsConsumer(MetricsListener.class, 2);
		conf.setDebug(false);
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args.length > 0) {
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
