package analytics;

import analytics.bolt.RealtyTracBolt;
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
import analytics.bolt.PrinterBolt;

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

      public JavaRealtyTracSpout()
      {
          super("python", "RealtyTracSpout.py");
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
          outputFieldsDeclarer.declare(new Fields("street", "city", "county", "state", "saleStatus", "date", "bed", "bath", "area", "price"));
          //7414 S Harvard Ave,Chicago,cook-county,IL,1,3/24/2014,5,2,3318,39000
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
          return null;
      }
  }


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new JavaRealtyTracSpout(), 1);

    builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("spout");
    builder.setBolt("mongo", new RealtyTracBolt(), 1).shuffleGrouping("spout");


      Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("realty-trac", conf, builder.createTopology());
      Thread.sleep(10000000);
      cluster.shutdown();
    }
  }
}
