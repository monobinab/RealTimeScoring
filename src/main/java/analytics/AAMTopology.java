package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ParsingBoltWebTraits;
import analytics.bolt.PersistTraitsBolt;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class AAMTopology {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AAMTopology.class);
	public static void main(String[] args){
		LOGGER.info("Starting aam traits topology");
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		String topic = TopicConstants.AAM_CDF_TRAITS; 
		int port = TopicConstants.PORT;
		TopologyBuilder builder = new TopologyBuilder();

	   	String[] servers = RedisConnection.getServers();
	    builder.setSpout("AAM_CDF_Traits1", new AAMRedisPubSubSpout(servers[0], port, topic), 1);
	    builder.setSpout("AAM_CDF_Traits2", new AAMRedisPubSubSpout(servers[1], port, topic), 1);
	    builder.setSpout("AAM_CDF_Traits3", new AAMRedisPubSubSpout(servers[2], port, topic), 1);

	    builder.setBolt("ParsingBoltWebTraits", new ParsingBoltWebTraits(), 1)
	    .shuffleGrouping("AAM_CDF_Traits1").shuffleGrouping("AAM_CDF_Traits2").shuffleGrouping("AAM_CDF_Traits3");
	    builder.setBolt("strategy_bolt", new StrategyScoringBolt(),1).shuffleGrouping("ParsingBoltWebTraits");
	    builder.setBolt("persist_traits" , new PersistTraitsBolt(), 1).shuffleGrouping("ParsingBoltWebTraits");
	    builder.setBolt("score_publish_bolt", new ScorePublishBolt(servers[0], port,"score"), 1).shuffleGrouping("strategy_bolt", "score_stream");
	    builder.setBolt("member_publish_bolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 2).shuffleGrouping("strategy_bolt", "member_stream");
	      
	    Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(3);
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error(e.getClass() + ": " +  e.getMessage(), e);
			}
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("AAMTraitsTopology", conf,
					builder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();

		}
			}
}
