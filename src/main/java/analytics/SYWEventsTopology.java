package analytics;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.SpoutConfig;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.PersistBoostsBolt;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.RTSKafkaBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.SYWKafkaSpout;
import analytics.util.KafkaUtil;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.SystemUtility;
import analytics.util.TopicConstants;

public class SYWEventsTopology {
	private static final Logger LOGGER = LoggerFactory.getLogger(SYWEventsTopology.class);
	public static void main(String[] args) throws Exception{
		
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
		String sywKafkaTopic="rts_syw_Interactions";
		String zkroot_syw="syw_zkroot";
		String group_id = "syw_groupid";
		String kafkatopic = TopicConstants.RESCORED_MEMBERIDS_KAFKA_TOPIC;
		String env = System.getProperty(MongoNameConstants.IS_PROD);
		TopologyBuilder topologyBuilder = new TopologyBuilder();	
		
		try {
			SpoutConfig spoutConfig = null;
			spoutConfig = new KafkaUtil(env).getSpoutConfig(sywKafkaTopic, zkroot_syw, group_id);
			topologyBuilder.setSpout("sywKafkaSpout", new SYWKafkaSpout(spoutConfig), 1);
		} catch (ConfigurationException e) {
			LOGGER.error(e.getClass() + ": " + e.getMessage() +" STACKTRACE : "+ ExceptionUtils.getFullStackTrace(e));
			System.exit(0);	
		}
		
		topologyBuilder.setBolt("parseEventsBolt", new ParsingBoltSYW(System
				.getProperty(MongoNameConstants.IS_PROD)), 1)
				.shuffleGrouping("sywKafkaSpout");
		topologyBuilder.setBolt("processSYWEvents",
				new ProcessSYWInteractions(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping(
				"parseEventsBolt");
		topologyBuilder.setBolt("RTSKafkaBolt", new RTSKafkaBolt(System.getProperty(MongoNameConstants.IS_PROD),kafkatopic), 1)
		.shuffleGrouping("strategyScoringBolt","kafka_stream");
		topologyBuilder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System
				.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("processSYWEvents", "score_stream");
				
		topologyBuilder.setBolt("persistBolt", new PersistBoostsBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("processSYWEvents", "persist_stream");
				
		if(System.getProperty(MongoNameConstants.IS_PROD).equals("PROD")){
			topologyBuilder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
		}	
		
		Config conf = new Config();
		conf.put("metrics_topology", "Syw");
	    conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
	
	    if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf,
					topologyBuilder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("syw_topology", conf,
					topologyBuilder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
