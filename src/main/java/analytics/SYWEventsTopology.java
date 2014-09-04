package analytics;

import analytics.bolt.ParsingBoltSYW;
import analytics.bolt.ProcessSYWInteractions;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.SYWRedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class SYWEventsTopology {


  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    String[] servers = new String[]{"rtsapp301p.qa.ch3.s.com"/*,"rtsapp302p.qa.ch3.s.com","rtsapp303p.qa.ch3.s.com"*/};

    //Read REDIS
    //TODO: Change this to the read from where Karththikka's web app sends
    builder.setSpout("SYWEventsSpout", new SYWRedisSpout(servers[0], 6379, "Message"), 1);
    //Parse the JSON 
    builder.setBolt("ParseEventsBolt", new ParsingBoltSYW(),1).shuffleGrouping("SYWEventsSpout");
    //Get the div line and boost variable 
    builder.setBolt("ProcessSYWEvents" , new ProcessSYWInteractions(), 1).shuffleGrouping("ParseEventsBolt");
    builder.setBolt("strategy_bolt", new StrategyBolt(),1).shuffleGrouping("ProcessSYWEvents");
    builder.setBolt("scoring_bolt", new ScoringBolt(),1).shuffleGrouping("strategy_bolt");
     
    Config conf = new Config();

//TODO: UNCOMMENT THIS IF STATEMENT!!!
//    if (args != null && args.length > 0) {
//      conf.setNumWorkers(3);
//
//      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
//    }
//    else {
        conf.setDebug(false);
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("syw", conf, builder.createTopology());
      Thread.sleep(10000000);
      cluster.shutdown();
//    }
  }
}
