package analytics;

import analytics.bolt.RealtyTracBolt;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class RealtyTracTopology {
  public static class SplitSentence extends ShellBolt implements IRichBolt {

    public SplitSentence() {
      super("python", "splitsentence.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class JavaRealtyTracSpout extends ShellSpout implements IRichSpout {

      public JavaRealtyTracSpout(String scriptName)
      {
          super("python", scriptName);
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
          outputFieldsDeclarer.declare(new Fields("street", "city", "county", "state", "zip","saleStatus", "date", "bed", "bath", "area", "price"));
          //"515 Sandstone Trce","Prattville","autauga-county","AL","36066","1","3\/27\/2013","4","3","2980","0.440","249000"
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
          return null;
      }
  }


  public static void main(String[] args) throws Exception {
		System.clearProperty(MongoNameConstants.IS_PROD);
		if (args.length > 0) {
			System.setProperty(MongoNameConstants.IS_PROD, "true");
		}
	//TODO: Requires python libraries installed, mongo collection does not exist currently. Mongo objects should be changed to DAO
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new JavaRealtyTracSpout("RealtyTracSpoutForSold.py"), 1);
    builder.setSpout("spout2", new JavaRealtyTracSpout("RealtyTracSpoutForListed.py"), 1);


    //builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("spout").shuffleGrouping("spout2");
    builder.setBolt("mongo", new RealtyTracBolt(), 2).shuffleGrouping("spout").shuffleGrouping("spout2");


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
      cluster.submitTopology("realty-trac", conf, builder.createTopology());
      Thread.sleep(10000000);
      cluster.shutdown();
    }
  }
}
