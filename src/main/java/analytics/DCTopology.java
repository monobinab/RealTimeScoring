package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.*;
import analytics.bolt.FlumeRPCBolt;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltDC;
import analytics.bolt.MemberPublishBolt;
import analytics.bolt.PersistDCBolt;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.bolt.SywScoringBolt;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.SystemUtility;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;

public class DCTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(ParsingBoltDC.class);
	private static final long serialVersionUID = 1L;
	private static final int partition_num = 3;
	private static final int redis_port = 6379;

	public static void main(String[] args) {
		boolean isLocal = true;
		String topologyId = "";
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		} else {

		TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts("trprtelpacmapp1.vm.itg.corp.us.shldcorp.com:2181");
		// use topology Id as part of the consumer ID to make it unique
		SpoutConfig kafkaConfig = new SpoutConfig(hosts, "telprod_reqresp_log_output", "", "RTSConsumer"+topologyId);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		if(isLocal){
			//default is false, only set to true for developing or testing locally
			//kafkaConfig.forceFromStart = true;
		}
		builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("dcParsingBolt", new ParsingBoltDC(System
				.getProperty(MongoNameConstants.IS_PROD)), 2).localOrShuffleGrouping("kafkaSpout");
	    builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)),1).localOrShuffleGrouping("dcParsingBolt", "score_stream");
		builder.setBolt("dcPersistBolt", new PersistDCBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 1).localOrShuffleGrouping("dcParsingBolt", "persist_stream");
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			builder.setBolt("flumeLoggingBolt", new FlumeRPCBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
			}	
			
		//builder.setBolt("scorePublishBolt", new ScorePublishBolt(RedisConnection.getServers()[0], redis_port,"score"), partition_num).localOrShuffleGrouping("strategyScoringBolt", "score_stream");
		//builder.setBolt("memberPublishBolt", new MemberPublishBolt(RedisConnection.getServers()[0], redis_port,"member"), partition_num).localOrShuffleGrouping("strategyScoringBolt", "member_stream");
		Config conf = new Config();
		conf.put("metrics_topology", "DC");
		conf.registerMetricsConsumer(MetricsListener.class, partition_num);
		conf.setDebug(false);
		conf.put("topology_environment", System.getProperty(MongoNameConstants.IS_PROD));
		if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(partition_num);
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

}
