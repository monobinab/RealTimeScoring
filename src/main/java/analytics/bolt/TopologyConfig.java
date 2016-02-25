/**
 * 
 */
package analytics.bolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MetricsListener;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author spannal
 *
 */
public abstract class TopologyConfig {

	static String env;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TopologyConfig.class);
	
	/*public TopologyConfig(String source, String env, String topologyAliasName) {
		this.source = source;
		this.env = env;
		this.topologyAliasName = topologyAliasName;
		
	}*/
	
	public static Config prepareStormConf(String source){
		
		Config conf = new Config();
		conf.put("metrics_topology", source);
		conf.registerMetricsConsumer(MetricsListener.class, System.getProperty(MongoNameConstants.IS_PROD), 3);
		conf.setDebug(false);
		
		if (env.equalsIgnoreCase("PROD")
				|| env.equalsIgnoreCase("QA")) {

			conf.setNumWorkers(6);
			conf.setNumAckers(5);
			conf.setMessageTimeoutSecs(300);
			conf.setStatsSampleRate(1.0);
				
		}else{
			
			conf.setDebug(false);
			conf.setMaxTaskParallelism(3);
			
		}
		return conf;
	}
	
	public static Boolean setEnvironment(String[] args) {
		if (args.length > 0) {
			for (String argument : args) {

				if (argument.equalsIgnoreCase("PROD")) {
					System.setProperty(MongoNameConstants.IS_PROD, "PROD");
				} else if (argument.equalsIgnoreCase("QA")) {
					System.setProperty(MongoNameConstants.IS_PROD, "QA");
				} else if (argument.equalsIgnoreCase("LOCAL")) {
					System.setProperty(MongoNameConstants.IS_PROD, "LOCAL");
				}
			}
			env = System.getProperty(MongoNameConstants.IS_PROD);
			return true;
		} else {
			return false;
		}
	}
	
	
	public static void submitStorm(Config conf, TopologyBuilder builder, String topologyAliasName){
		
		if (env.equalsIgnoreCase("PROD")
				|| env.equalsIgnoreCase("QA")) {	
			try {
				StormSubmitter.submitTopology(topologyAliasName, conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				LOGGER.error("Error in Topology: " + topologyAliasName);
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			} catch (InvalidTopologyException e) {
				LOGGER.error("Error in Topology: " + topologyAliasName);
				LOGGER.error(e.getClass() + ": " + e.getMessage(), e);
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyAliasName, conf, builder.createTopology());
			try {
				Thread.sleep(10000000);
			} catch (InterruptedException e) {
				LOGGER.error("Error in Topology: " + topologyAliasName);
				LOGGER.debug("Unable to wait for topology", e);
			}
			cluster.shutdown();
		}
		
	}
	
}
