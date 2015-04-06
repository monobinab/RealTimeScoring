/*package analytics;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltPOS;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class RealTimeScoringTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(RealTimeScoringTopology.class);

	public static void main(String[] args) throws ConfigurationException {
		//System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential("POS");
		if(mqCredential==null){
			LOGGER.error("Unable to get a MQ connections");
			return;
		}
		topologyBuilder
		.setSpout(
				"npos1",
				new WebsphereMQSpout(mqCredential.getHostOneName(),
						mqCredential.getPort(), mqCredential
								.getQueueOneManager(), mqCredential
								.getQueueChannel(), mqCredential
								.getQueueName()), 2);
		topologyBuilder
		.setSpout(
				"npos2",
				new WebsphereMQSpout(mqCredential.getHostTwoName(),
						mqCredential.getPort(), mqCredential
								.getQueueTwoManager(), mqCredential
								.getQueueChannel(), mqCredential
								.getQueueName()), 2);

		topologyBuilder.setBolt("parsing_bolt", new ParsingBoltPOS())
				.shuffleGrouping("npos1").shuffleGrouping("npos2");
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt())
				.shuffleGrouping("parsing_bolt");
		topologyBuilder.setBolt("score_publish_bolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 2).shuffleGrouping("strategy_bolt","score_stream");
		Config conf = new Config();
		conf.setDebug(false);
	//	conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("realtimescoring_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
			cluster.shutdown();
		}
	}
}
*/