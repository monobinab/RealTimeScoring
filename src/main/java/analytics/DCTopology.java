package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.*;
import analytics.bolt.DCParsingBolt;
import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.SywScoringBolt;
import analytics.util.RedisConnection;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;

public class DCTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(DCParsingBolt.class);
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		boolean isLocal = true;
		String topologyId = "";
		if(args.length > 0){
			isLocal = false;
			topologyId = args[0];
		}

		TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts("trprtelpacmapp1.vm.itg.corp.us.shldcorp.com:2181");
		// use topology Id as part of the consumer ID to make it unique
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, "telprod_reqresp_log_output", "", "RTSConsumer"+topologyId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.forceFromStart = true;
		// TODO: partition number better be dynamic
		builder.setSpout("kafka_spout", new KafkaSpout(kafkaConfig), 3);
		builder.setBolt("DCParsing_Bolt", new DCParsingBolt(), 3).localOrShuffleGrouping("kafka_spout");
		builder.setBolt("scoringBolt", new SywScoringBolt(), 3).localOrShuffleGrouping("DCParsing_Bolt");
		builder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], 6379,"score"), 3).localOrShuffleGrouping("scoringBolt", "score_stream");
		builder.setBolt("member_publish_bolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 3).localOrShuffleGrouping("scoringBolt", "member_stream");
		Config conf = new Config();
		conf.setDebug(false);

		if (!isLocal) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("DCTopology", conf, builder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();
		}

	}

}
