package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.*;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.GlobalPartitionInformation;
import analytics.bolt.DCParsingBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import backtype.storm.spout.SchemeAsMultiScheme;

public class DCTopology {
	// private static final Logger LOGGER = LoggerFactory
	// .getLogger(DCParsingBolt.class);
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {

		TopologyBuilder topologyBuilder = new TopologyBuilder();
		//int partitionNum = 6;
		//static partition
		GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation();
//		for (int i = 1; i < partitionNum; i++) {
//			//if (i == 1 || i == 5)
//			partitionInfo.addPartition(i-1, new storm.kafka.Broker("trprtelpaquapp" + Integer.toString(i) + ".vm.itg.corp.us.shldcorp.com", 9092));
//		}
		//dynamic partition, however partition info only stored the first three servers
		BrokerHosts broker = new ZkHosts("trprtelpacmapp1.vm.itg.corp.us.shldcorp.com:2181");
		partitionInfo.addPartition(0, new storm.kafka.Broker("trprtelpaquapp1.vm.itg.corp.us.shldcorp.com", 9092));

		StaticHosts hosts = new StaticHosts(partitionInfo);

		SpoutConfig kafkaConfig = new SpoutConfig(broker, "telprod_reqresp_log_output", "/brokers/topics", "RTSConsumer");
		//kafkaConfig.bufferSizeBytes = 64*1024;
		//kafkaConfig.fetchSizeBytes = 64*1024;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		KafkaSpout spout = new KafkaSpout(kafkaConfig);
		
		topologyBuilder.setSpout("kafka_spout", spout, 3);
		topologyBuilder.setBolt("DCParsing_Bolt", new DCParsingBolt(), 3).shuffleGrouping("kafka_spout");

		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);
		config.setDebug(false);
		// will it be automatically shutdown if no message is coming?
		// find a way to identify the exeception thrown before program exits

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("DCTopology", config, topologyBuilder.createTopology());
		try {
			Thread.sleep(10000000);
		} catch (InterruptedException e) {
			// LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			e.printStackTrace();
		}
		// cluster.shutdown();

		// TODO: enable stormsubmitter so it does not only run locally

	}

}
