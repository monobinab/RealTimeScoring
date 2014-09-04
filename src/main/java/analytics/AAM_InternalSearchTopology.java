package analytics;

import analytics.bolt.ParsingBoltAAM_InternalSearch;
import analytics.spout.AAMRedisPubSubSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;


public class AAM_InternalSearchTopology {
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		String[] topics = new String[]{
		// "AAM_CDF_SID",
		// "AAM_CDF_APP_Performance",
		// "AAM_CDF_PSID",
		// "AAM_CDF_Products",
		// "AAM_CDF_PaidSearch",
		// "AAM_CDF_NaturalSearch",
		"AAM_CDF_InternalSearch"
		// "AAM_CDF_CheckoutProducts",
		// "AAM_CDF_ATCProducts",
		// "AAM_CDF_AbanCart",
		// "AAM_CDF_AbanBrow",
		// "AAM_CDF_Traits"
		};
		
		String[] servers = new String[]{"rtsapp301p.qa.ch3.s.com","rtsapp302p.qa.ch3.s.com","rtsapp303p.qa.ch3.s.com"};
		
		
//		for(String topic:topics){
//			for(String server:servers)
//			{
//				builder.setSpout(topic+server, new AAMRedisPubSubSpout(server, 6379, topic), 1);
//			}
//		}
		
//		BoltDeclarer boltDeclarer = builder.setBolt("ParsingBoltAAM_InternalSearch", new ParsingBoltAAM_InternalSearch(), 1);

		
		builder.setSpout("AAM_CDF_InternalSearch1", new AAMRedisPubSubSpout("rtsapp301p.qa.ch3.s.com", 6379, "AAM_CDF_InternalSearch"), 1);
		builder.setSpout("AAM_CDF_InternalSearch2", new AAMRedisPubSubSpout("rtsapp302p.qa.ch3.s.com", 6379, "AAM_CDF_InternalSearch"), 1);
		builder.setSpout("AAM_CDF_InternalSearch3", new AAMRedisPubSubSpout("rtsapp303p.qa.ch3.s.com", 6379, "AAM_CDF_InternalSearch"), 1);
		
		builder.setBolt("ParsingBoltAAM_InternalSearch1", new ParsingBoltAAM_InternalSearch(), 1).shuffleGrouping("AAM_CDF_InternalSearch1");
		builder.setBolt("ParsingBoltAAM_InternalSearch2", new ParsingBoltAAM_InternalSearch(), 1).shuffleGrouping("AAM_CDF_InternalSearch2");
		builder.setBolt("ParsingBoltAAM_InternalSearch3", new ParsingBoltAAM_InternalSearch(), 1).shuffleGrouping("AAM_CDF_InternalSearch3");

		//BoltDeclarer boltDeclarer = 
//		builder.setBolt("strategy_bolt", new StrategyBolt(),1).shuffleGrouping("ParsingBoltAAM_InternalSearch");
//		builder.setBolt("scoring_bolt", new ScoringBolt(),1).shuffleGrouping("strategy_bolt");
//		builder.setBolt("RankPublishBolt", new RankPublishBolt("rtsapp401p.prod.ch4.s.com", 6379,"score"), 1).shuffleGrouping("scoring_bolt");
		
		
//		for(String topic:topics){
//			for(String server:servers)
//			{
//				boltDeclarer.fieldsGrouping(topic + server, new Fields("uuid"));
//			}
//		}
		//builder.setBolt("print", new RealtyTracBolt(), 2).shuffleGrouping("spout").shuffleGrouping("spout2");
		
//		boltDeclarer1.fieldsGrouping("AAM_CDF_InternalSearch1", new Fields("uuid"));
//		boltDeclarer2.fieldsGrouping("AAM_CDF_InternalSearch2", new Fields("uuid"));
//		boltDeclarer3.fieldsGrouping("AAM_CDF_InternalSearch3", new Fields("uuid"));
		
		Config conf = new Config();
		
		
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("aam", conf, builder.createTopology());
			Thread.sleep(10000000);
			cluster.shutdown();
		}
	}
}
