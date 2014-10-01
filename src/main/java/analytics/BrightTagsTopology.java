package analytics;

import analytics.bolt.PrinterBolt;
import analytics.spout.RedisPubSubSpout;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class BrightTagsTopology {


  public static void main(String[] args) throws Exception {
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new RedisPubSubSpout("rtsapp302p.qa.ch3.s.com", 6379, "Products"), 1);

    builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("spout");
    //builder.setBolt("print", new RealtyTracBolt(), 2).shuffleGrouping("spout").shuffleGrouping("spout2");


    Config conf = new Config();
	conf.put(MongoNameConstants.IS_PROD, System.getProperty(MongoNameConstants.IS_PROD));


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
        conf.setDebug(true);
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("brightTags", conf, builder.createTopology());
      Thread.sleep(10000000);
      cluster.shutdown();
    }
  }
}
