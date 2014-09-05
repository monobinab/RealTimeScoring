package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.ParsingBoltWebTraits;
import analytics.bolt.PersistTraitsBolt;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.AAMRedisPubSubSpout;
import analytics.util.Logging;
import analytics.util.RedisConnection;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class AAMTopology {
	static final Logger logger = LoggerFactory
			.getLogger(AAMTopology.class);

  public static void main(String[] args) throws Exception {
	Logging.creatLogger("AAMTraitsTopology.log");
    TopologyBuilder builder = new TopologyBuilder();

   	String[] servers = RedisConnection.getServers();
    builder.setSpout("AAM_CDF_Traits1", new AAMRedisPubSubSpout(servers[0], 6379, "AAM_CDF_Traits"), 1);
    builder.setSpout("AAM_CDF_Traits2", new AAMRedisPubSubSpout(servers[1], 6379, "AAM_CDF_Traits"), 1);
    builder.setSpout("AAM_CDF_Traits3", new AAMRedisPubSubSpout(servers[2], 6379, "AAM_CDF_Traits"), 1);

    builder.setBolt("ParsingBoltWebTraits", new ParsingBoltWebTraits(), 1)
    .shuffleGrouping("AAM_CDF_Traits1").shuffleGrouping("AAM_CDF_Traits2").shuffleGrouping("AAM_CDF_Traits3");
    builder.setBolt("strategy_bolt", new StrategyBolt(),1).shuffleGrouping("ParsingBoltWebTraits");
    builder.setBolt("persist_traits" , new PersistTraitsBolt(), 1).shuffleGrouping("ParsingBoltWebTraits");
    builder.setBolt("scoring_bolt", new ScoringBolt(),1).shuffleGrouping("strategy_bolt");
    builder.setBolt("score_publish_bolt", new ScorePublishBolt(servers[0], 6379,"score"), 1).shuffleGrouping("scoring_bolt");
      
    Config conf = new Config();
	conf.setDebug(false);
	conf.setNumWorkers(3);
	if (args.length > 0) {
		try {
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cluster.shutdown();

	}
  }
}
