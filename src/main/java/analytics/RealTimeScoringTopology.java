package analytics;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltPOS;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class RealTimeScoringTopology {

	static final Logger logger = LoggerFactory
			.getLogger(RealTimeScoringTopology.class);

	public static void main(String[] args) throws ConfigurationException {

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential();

		topologyBuilder.setSpout(
				"npos1",
				new WebsphereMQSpout(mqCredential.getHostRtsThreeName(),
						mqCredential.getPort(), mqCredential
								.getQueueRtsThreeManager(), mqCredential
								.getQueueRts2Channel(), mqCredential
								.getQueueRts2Name()), 1);
		topologyBuilder.setSpout(
				"npos2",
				new WebsphereMQSpout(mqCredential.getHostRtsFourName(),
						mqCredential.getPort(), mqCredential
								.getQueueRtsFourManager(), mqCredential
								.getQueueRts2Channel(), mqCredential
								.getQueueRts2Name()), 1);

		topologyBuilder.setBolt("parsing_bolt", new ParsingBoltPOS())
				.shuffleGrouping("npos1").shuffleGrouping("npos2");
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt())
				.shuffleGrouping("parsing_bolt");

		Config conf = new Config();
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				logger.error(e.getClass() + ": " + e.getMessage(), e);
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
				logger.error(e.getClass() + ": " + e.getMessage(), e);
			}
			cluster.shutdown();

		}
	}
}
