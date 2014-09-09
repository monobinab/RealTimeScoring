package analytics;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltPOS;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.WebsphereMQSpout;
import analytics.util.MQConnectionConfig;
import analytics.util.WebsphereMQCredential;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

import com.mongodb.DBObject;

public class RealTimeScoringTopology {

	static final Logger logger = LoggerFactory
			.getLogger(RealTimeScoringTopology1.class);

	public static void main(String[] args) throws ConfigurationException {

		TopologyBuilder topologyBuilder = new TopologyBuilder();

		MongoObjectGrabber mongoMapper = new MongoObjectGrabber() {
			@Override
			public List<Object> map(DBObject object) {
				if (object != null)
					System.out.println(" in Mapper: " + object);
				List<Object> tuple = new ArrayList<Object>();
				tuple.add(object);
				return tuple;
			}

			@Override
			public String[] fields() {
				return new String[] { "document" };
			}
		};

		MQConnectionConfig mqConnection = new MQConnectionConfig();
		WebsphereMQCredential mqCredential = mqConnection
				.getWebsphereMQCredential();

		topologyBuilder.setSpout(
				"npos1",
				new WebsphereMQSpout(mqCredential.getHostThreeName(),
						mqCredential.getPort(), mqCredential
								.getQueueThreeManager(), mqCredential
								.getQueue2Channel(), mqCredential
								.getQueue2Name()), 1);
		topologyBuilder.setSpout(
				"npos2",
				new WebsphereMQSpout(mqCredential.getHostFourName(),
						mqCredential.getPort(), mqCredential
								.getQueueFourManager(), mqCredential
								.getQueue2Channel(), mqCredential
								.getQueue2Name()), 1);

		topologyBuilder.setBolt("parsing_bolt", new ParsingBoltPOS())
				.shuffleGrouping("npos1").shuffleGrouping("npos2");
		topologyBuilder.setBolt("strategy_bolt", new StrategyBolt())
				.shuffleGrouping("parsing_bolt");
		topologyBuilder.setBolt("scoring_bolt", new ScoringBolt())
				.shuffleGrouping("strategy_bolt");

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
