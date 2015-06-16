package analytics;

//import analytics.bolt.SignalBolt;
import analytics.bolt.PrinterBolt;
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
public class SignalTopology {
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

  public static class JavaSignalSpout extends ShellSpout implements IRichSpout {

      public JavaSignalSpout(String scriptName)
      {
          super("python", scriptName);
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
          outputFieldsDeclarer.declare(new Fields("channel", "product", "time", "uuid", "sywrID"));
          //"515 Sandstone Trce","Prattville","autauga-county","AL","36066","1","3\/27\/2013","4","3","2980","0.440","249000"
      }

      @Override
      public Map<String, Object> getComponentConfiguration() {
          return null;
      }
  }

  public static void main(String[] args) throws Exception {

	    TopologyBuilder builder = new TopologyBuilder();

	    builder.setSpout("spout", new JavaSignalSpout("signalsp.py"), 1);
	    builder.setBolt("print", new PrinterBolt(), 1).shuffleGrouping("spout");
	    //builder.setBolt("mongo", new RealtyTracBolt(), 2).shuffleGrouping("spout").shuffleGrouping("spout2");
	    //builder.setBolt("signalOut", new JavaSignalBolt("singalBolt.py"), 2).shuffleGrouping("spout");


	    Config conf = new Config();



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
