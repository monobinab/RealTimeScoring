package analytics;

import analytics.bolt.FacebookBolt;
import analytics.bolt.ScoringBolt;
import analytics.bolt.StrategyBolt;
import analytics.spout.FacebookRedisSpout;
import analytics.spout.SYWRedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class SocialTopology {
public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
	TopologyBuilder builder = new TopologyBuilder();
	 String[] servers = new String[]{"rtsapp301p.qa.ch3.s.com"/*,"rtsapp302p.qa.ch3.s.com","rtsapp303p.qa.ch3.s.com"*/};

	builder.setSpout("facebookSpout", new FacebookRedisSpout(servers[0], 6379, "FB"), 1);
	builder.setBolt("facebookBolt", new FacebookBolt()).shuffleGrouping("facebookSpout");
	builder.setBolt("strategyBolt", new StrategyBolt(),1).shuffleGrouping("facebookBolt");
    builder.setBolt("scoringBolt", new ScoringBolt(),1).shuffleGrouping("strategyBolt");
	Config conf = new Config();

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {
        conf.setDebug(false);
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("social", conf, builder.createTopology());
      Thread.sleep(10000000);
      cluster.shutdown();
    }
}
}
