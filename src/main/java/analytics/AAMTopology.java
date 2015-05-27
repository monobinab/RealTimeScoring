package analytics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.bolt.LoggingBolt;
import analytics.bolt.ParsingBoltWebTraits;
import analytics.bolt.PersistTraitsBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.spout.WebHDFSSpout;
import analytics.util.Constants;
import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import analytics.util.RedisConnection;
import analytics.util.SystemUtility;
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
		if (!SystemUtility.setEnvironment(args)) {
			System.out
					.println("Please pass the environment variable argument- 'PROD' or 'QA' or 'LOCAL'");
			System.exit(0);
		}
			TopologyBuilder builder = new TopologyBuilder();

	   	String[] servers = RedisConnection.getServers(System.getProperty(MongoNameConstants.IS_PROD));
	  
	   	//Sree. Spout that wakes up every 5 mins and process the Traits
	  	builder.setSpout("traitsSpout", new WebHDFSSpout(servers[1], TopicConstants.PORT, Constants.AAM_TRAITS_PATH, "aamTraits"), 1);
	  	builder.setBolt("parsingBoltWebTraits", new ParsingBoltWebTraits(System.getProperty(MongoNameConstants.IS_PROD), "aamTraits"), 1)
	  		.shuffleGrouping("traitsSpout");
	  	builder.setBolt("strategyScoringBolt", new StrategyScoringBolt(System.getProperty(MongoNameConstants.IS_PROD)),1).shuffleGrouping("parsingBoltWebTraits");
	    builder.setBolt("persistTraits" , new PersistTraitsBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("parsingBoltWebTraits");
	    if(System.getProperty(MongoNameConstants.IS_PROD).equalsIgnoreCase("PROD")){
	    	builder.setBolt("loggingBolt", new LoggingBolt(System.getProperty(MongoNameConstants.IS_PROD)), 1).shuffleGrouping("strategyScoringBolt", "score_stream");
        }

	    Config conf = new Config();
		conf.put("metrics_topology", "AamTraits");
	    conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		conf.setDebug(false);
		conf.setNumWorkers(3);
		if (System.getProperty(MongoNameConstants.IS_PROD)
				.equalsIgnoreCase("PROD")
				|| System.getProperty(MongoNameConstants.IS_PROD)
						.equalsIgnoreCase("QA")) {
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
