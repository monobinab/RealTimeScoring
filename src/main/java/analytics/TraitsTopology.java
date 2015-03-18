package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.FlumeRPCBolt;
import analytics.bolt.ParsingBoltWebTraits;
import analytics.bolt.PersistTraitsBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.WebHDFSSpout;
import analytics.spout.Write2HDFSSpout;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.TopicConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;



public class TraitsTopology{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TraitsTopology.class);
	
	public static void main(String[] args)  throws Exception{
		LOGGER.info("starting Traits topology");
		
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
		
		TopologyBuilder builder = new TopologyBuilder();
		String[] servers = RedisConnection.getServers();
		
		//Spout that wakes up every 5 mins and process the Traits
		builder.setSpout("traitsSpout", new Write2HDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_TRAITS_PATH, 
					"aamTraits","/user/spannal/test/temp.txt"), 1);
		
		builder.setBolt("parsingBoltWebTraits", new ParsingBoltWebTraits(), 1)
	    .shuffleGrouping("traitsSpout");
		
		
		Config conf = new Config();
		conf.put("traits_topology", "Traits");
		conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("traits_topology", conf,
					builder.createTopology());
			Thread.sleep(1000000);
			cluster.shutdown();
		}

	}
}
	


