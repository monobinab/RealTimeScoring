package analytics.integration;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;
import analytics.bolt.MemberPublishBolt;
import analytics.bolt.ParsingBoltPOS;
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

public class AAMTraitsTestTopology {
/**
 * bz+Vf3U+/Ujjwzjv5a322hdJCZ0=, {"M_WEB_TRAIT_POWER_TOOL_8_14":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_DAY_HAND_TOOL_0_7":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_TRAIT_POWER_TOOL_0_7":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_TRAIT_TRACTOR_15_30":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}"}, WebTraits]
emited [Cieh1BvT91qFFXXtzzSU1GFZjfk=, {"M_WEB_TRAIT_POWER_TOOL_8_14":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_DAY_HAND_TOOL_0_7":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_TRAIT_POWER_TOOL_0_7":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}","M_WEB_TRAIT_TRACTOR_15_30":"{\"2014-10-06\":[\"271359\",\"271981\",\"270740\",\"206649\",\"211056\",\"270759\",\"80785\",\"80638\",\"81836\",\"80658\"]}"}, WebTraits]

 */
	public static void main(String[] args) {
		
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "test");
		}
		System.setProperty(MongoNameConstants.IS_PROD, "test");
		
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("test_spout", new MockAAMSpout());
	    builder.setBolt("strategy_bolt", new StrategyScoringBolt(),1).shuffleGrouping("test_spout");
	    builder.setBolt("persist_traits" , new PersistTraitsBolt(), 1).shuffleGrouping("test_spout");
	  //  builder.setBolt("score_publish_bolt", new ScorePublishBolt(servers[0], port,"score"), 1).shuffleGrouping("strategy_bolt", "score_stream");
	  //  builder.setBolt("member_publish_bolt", new MemberPublishBolt(RedisConnection.getServers()[0], 6379,"member"), 2).shuffleGrouping("strategy_bolt", "member_stream");
	      
	    Config conf = new Config();
		conf.setDebug(false);
		conf.setNumWorkers(3);
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("AAMTraitsTopology", conf,
					builder.createTopology());
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				System.out.println(e.getMessage());
			}
			cluster.shutdown();

		}
			

		
}
	
	

