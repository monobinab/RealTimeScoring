package analytics.integration;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Test;

import com.mongodb.DB;

import analytics.bolt.ParsingBoltPOS;
import analytics.bolt.ScorePublishBolt;
import analytics.bolt.StrategyScoringBolt;
import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class RealTimeTopologyTest {
	@Test
	public void testWithValidRecord() throws ConfigurationException{
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
        topologyBuilder.setBolt("score_check_test",new GenericScoreCheckBolt(expected), 4).shuffleGrouping("strategy_scoring_bolt", "score_stream");
		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);
		conf.put(MongoNameConstants.IS_PROD, "test");
		conf.put("nimbus.host", "test");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("realtimescoring_topology", conf,
				topologyBuilder.createTopology());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			System.out.println(e.getClass() + ": " + e.getMessage());
		}
		Map<String,Object> actual = GenericScoreCheckBolt.getActualResults();
		for(String key:expected.keySet()){
			Assert.assertEquals(expected.get(key), actual.get(key));
		}
		if(!System.getProperty("os.name").startsWith("Windows"))
			cluster.shutdown();//This fails on windows. Known issue
	}
	@After
	public void cleanUp() throws ConfigurationException{
		DB db = DBConnection.getDBConnection();
		db.dropDatabase();
	}
}
