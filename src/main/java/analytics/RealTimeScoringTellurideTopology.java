package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.*;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
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

	static final Logger logger = LoggerFactory
			.getLogger(RealTimeScoringTellurideTopology.class);
	
	
	public static void main(String[] args) throws ConfigurationException {
		logger.info("Starting telluride real time scoring topology");
		// Configure logger
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential();

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
		topologyBuilder.setBolt("parsing_bolt", new TellurideParsingBoltPOS(), 4)
				.shuffleGrouping("telluride1").shuffleGrouping("telluride2");
        topologyBuilder.setBolt("strategy_scoring_bolt", new StrategyScoringBolt(), 4).shuffleGrouping("parsing_bolt");
        //Redis publish to server 1
        topologyBuilder.setBolt("score_publish_bolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 2).shuffleGrouping("strategy_scoring_bolt");


		Config conf = new Config();
		conf.setDebug(false);

		if (args.length > 0) {
			try {
                conf.setNumAckers(5);
                conf.setMessageTimeoutSecs(300);
                conf.setStatsSampleRate(1.0);
                conf.setNumWorkers(5);
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error(e.getClass() + ": " +  e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				logger.error(e.getClass() + ": " +  e.getMessage(), e);
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
				logger.error(e.getClass() + ": " +  e.getMessage(), e);
			}
			cluster.shutdown();

		}
	}
}
