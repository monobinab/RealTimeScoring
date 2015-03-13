package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import analytics.bolt.ParsingBoltOccassion;
import analytics.bolt.PersistOccasionBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.OccassionRedisSpout;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.TopicConstants;

public class PurchaseOccassionTopology{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(PurchaseOccassionTopology.class);

	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting purchase occassion topology");
		System.clearProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		String topic = TopicConstants.OCCASSION;

		topologyBuilder.setSpout("occassionSpout1", new OccassionRedisSpout(
				0, "Member_Sample_Tags"), 1);
		topologyBuilder.setSpout("occassionSpout2", new OccassionRedisSpout(
				1, "Member_Sample_Tags"), 1);
		topologyBuilder.setSpout("occassionSpout3", new OccassionRedisSpout(
				2, "Member_Sample_Tags"), 1);

		topologyBuilder.setBolt("parseOccassionBolt", new ParsingBoltOccassion(), 1)
		.shuffleGrouping("occassionSpout1").shuffleGrouping("occassionSpout2").shuffleGrouping("occassionSpout3");
		topologyBuilder.setBolt("persistOccasionBolt", new PersistOccasionBolt(), 1)
		.shuffleGrouping("parseOccassionBolt", "persist_stream");
		topologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(), 1)
		.shuffleGrouping("parseOccassionBolt");

		Config conf = new Config();
		conf.put("metrics_topology", "PurchaseOccasion");
	    conf.registerMetricsConsumer(MetricsListener.class, 3);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf,
					topologyBuilder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("occassion_topology", conf,
					topologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
		}
	}