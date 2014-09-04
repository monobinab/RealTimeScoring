package analytics;

import org.apache.commons.configuration.ConfigurationException;

import analytics.bolt.*;
import org.apache.log4j.Logger;

import analytics.spout.WebsphereMQSpout;
import analytics.util.Logging;
import analytics.util.MQConnectionConfig;
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

	static final Logger logger = Logger
			.getLogger(RealTimeScoringTellurideTopology.class);

	public static void main(String[] args) throws ConfigurationException {

		// Configure logger
		Logging.creatLogger("RealTimeScoringTellurideLog.log");
		TopologyBuilder topologyBuilder = new TopologyBuilder();



		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential();

		topologyBuilder
				.setSpout(
						"npos1",
						new WebsphereMQSpout(mqCredential.getHostOneName(),
								mqCredential.getPort(), mqCredential
										.getQueueOneManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 1);
		topologyBuilder
				.setSpout(
						"npos2",
						new WebsphereMQSpout(mqCredential.getHostTwoName(),
								mqCredential.getPort(), mqCredential
										.getQueueTwoManager(), mqCredential
										.getQueueChannel(), mqCredential
										.getQueueName()), 1);

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsing_bolt", new TellurideParsingBoltPOS())
				.shuffleGrouping("npos1").shuffleGrouping("npos2");
        topologyBuilder.setBolt("strategy_bolt", new StrategyBolt()).shuffleGrouping("parsing_bolt");
        topologyBuilder.setBolt("scoring_bolt", new ScoringBolt()).shuffleGrouping("strategy_bolt");
        //TODO: Change hardcoded redis
        topologyBuilder.setBolt("score_publish_bolt", new ScorePublishBolt("rtsapp401p.prod.ch4.s.com", 6379,"score")).shuffleGrouping("scoring_bolt");


		Config conf = new Config();
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("meetup_topology", conf,
					topologyBuilder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			cluster.shutdown();

		}
	}
}
