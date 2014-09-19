package analytics.integration;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.bolt.ParsingBoltPOS;
import analytics.bolt.StrategyScoringBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class RealTimeTopologyTest {
	static LocalCluster cluster; 
	static boolean testComplete;
	@BeforeClass
	public static void initializeCluster(){
		cluster = new LocalCluster();
		testComplete=false;
	}
	
	public static void shutDown(){
		//Windows cluster shutdown wont work, manually kill the test
		cluster.shutdown();
		testComplete=true;
	}
	
	@Test(timeout=10000)
	public void testWithValidRecord(){
		TestHelper.initializeDBForTests();
		Map<String,Object> expected = new HashMap<String, Object>();
		expected.put("l_id","1hGa3VmrRXWbAcwTcw0qw6BfzS4=");
		expected.put("newScore",0.04420650204669837);
		expected.put("model","59");
		expected.put("source","NPOS");
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("test_spout", new MockRealTimePOSSpout());

		// create definition of main spout for queue 1
		topologyBuilder.setBolt("parsing_bolt", new ParsingBoltPOS()).shuffleGrouping("test_spout");
        topologyBuilder.setBolt("strategy_scoring_bolt", new StrategyScoringBolt(), 4).shuffleGrouping("parsing_bolt");
        topologyBuilder.setBolt("score_check_test", new GenericScoreCheckBolt(expected), 4).shuffleGrouping("strategy_scoring_bolt");
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);
		cluster.submitTopology("realtimescoring_topology", conf,
				topologyBuilder.createTopology());
		while(!testComplete){
			//do nothing
		}
		System.out.println("complete.....");
	}
}
