package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.SYWRedisSpout;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class SYWEventsTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SYWEventsTopology.class);

	public static void main(String[] args) throws Exception {
		LOGGER.info("starting syw events topology");

		TopologyBuilder toplologyBuilder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers();
		String topic = TopicConstants.SYW;

		toplologyBuilder.setSpout("SYWEventsSpout", new SYWRedisSpout(
				servers[0], TopicConstants.PORT, topic), 1);
		// Parse the JSON
		toplologyBuilder.setBolt("ParseEventsBolt", new ParsingBoltSYW(), 1)
				.shuffleGrouping("SYWEventsSpout");
		// Get the div line and boost variable
		toplologyBuilder.setBolt("ProcessSYWEvents",
				new ProcessSYWInteractions(), 1).shuffleGrouping(
				"ParseEventsBolt");
		toplologyBuilder.setBolt("strategy_bolt", new StrategyScoringBolt(), 1)
				.shuffleGrouping("ProcessSYWEvents");

		Config conf = new Config();

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,
					toplologyBuilder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("syw_topology", conf,
					toplologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
