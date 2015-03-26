package analytics.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.MongoNameConstants;
import analytics.util.SystemPropertyUtility;
import analytics.util.SywApiCalls;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ProcessSywBoltMockTest {
	/**
	 * WE ARE NOT STUBBING OUT SYWAPICALLS
	 */
	/*static Map<String,String> conf;
	static DB db;*/
	@BeforeClass
	public static void initializeFakeMongo() throws ConfigurationException{
		/*System.setProperty("rtseprod", "test");
		conf = new HashMap<String, String>();
        conf.put("rtseprod", "test");
        conf.put("nimbus.host","test");
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));	
		db = DBConnection.getDBConnection();*/
		
		SystemPropertyUtility.setSystemProperty();
	}
	@Test
	public void onlyCertainCatalogTypesAreProcessed(){		
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        ProcessSYWInteractions boltUnderTest = new ProcessSYWInteractions(System.getProperty(MongoNameConstants.IS_PROD));
       TopologyContext context = new MockTopologyContext();
        boltUnderTest.prepare(SystemPropertyUtility.getStormConf(),context , outputCollector);
        String lId = "xo0b7SN1eER9shCSj0DX+eSGag=";
		String interactionType = "AddToCatalog";
		String interactionString = "{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":184008680,\"EntityType\":\"Product\"},{\"Id\":8353466,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}";
	
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockInteractionTuple(lId, interactionString, interactionType);
        
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple().get("persist_stream");
        Assert.assertNull(outputTuple);
	}
	@Ignore
	@Test
	public void standardCatalogsAreProcessed() throws ConfigurationException{
		/*
		 *     15009844,    15009912,    16881520 - like, want and own catalog ids
		 */
		DB conn = DBConnection.getDBConnection();
		
		DBCollection pidDivLn = conn.getCollection("pidDivLn");
		
		DBCollection modelSywBoosts = conn.getCollection("modelSywBoosts");
		modelSywBoosts.insert(new BasicDBObject("m",34).append("b", "BOOST_SYW_OWN_HA_ALL_TCOUNT"));
		modelSywBoosts.insert(new BasicDBObject("m",57).append("b", "BOOST_SYW_OWN_REGRIG_TCOUNT"));

		DBCollection memberScore = conn.getCollection("memberScore");
		memberScore.insert(new BasicDBObject("l_id","do0b7SN1eER9shCSj0DX+eSGag=").append("34", 0.0079098).append("57", 0.00213123));
		

		
		DBCollection modelPercentile = conn.getCollection("modelPercentile");
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","90").append("maxScore", "0.0158418"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","91").append("maxScore", "0.0172141"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","92").append("maxScore", "0.0188900"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","93").append("maxScore", "0.0209311"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","94").append("maxScore", "0.0234838"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","95").append("maxScore", "0.0268622"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","96").append("maxScore", "0.0314226"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","97").append("maxScore", "0.0382990"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","98").append("maxScore", "0.0512915"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","99").append("maxScore", "0.1304244"));
		modelPercentile.insert(new BasicDBObject("modelId","34").append("modelName", "BOOST_SYW_OWN_HA_ALL_TCOUNT").append("modelDesc", "Home Appliance").append("percentile","50").append("maxScore", "0.0033978"));

		
		String pid = new SywApiCalls().getCatalogId(280987671);
		pidDivLn.insert(new BasicDBObject("pid",pid).append("d","046").append("l","04601"));
		DBCollection divLnBoost = conn.getCollection("divLnBoost");
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_WANT_REGRIG_TCOUNT"));
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_WANT_HA_ALL_TCOUNT"));
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_LIKE_HA_ALL_TCOUNT"));
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_LIKE_REGRIG_TCOUNT"));
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_OWN_HA_ALL_TCOUNT"));
		divLnBoost.insert(new BasicDBObject("d","04601").append("b", "BOOST_SYW_OWN_REGRIG_TCOUNT"));
		DBCollection feedBoosts = conn.getCollection("feedBoosts");
		BasicDBList boosts = new BasicDBList();
		boosts.add("BOOST_SYW_LIKE_REGRIG_TCOUNT");
		boosts.add("BOOST_SYW_LIKE_HA_ALL_TCOUNT");
		feedBoosts.insert(new BasicDBObject("f","SYW_LIKE").append("b",boosts));
		boosts = new BasicDBList();
		boosts.add("BOOST_SYW_OWN_REGRIG_TCOUNT");
		boosts.add("BOOST_SYW_OWN_HA_ALL_TCOUNT");
		feedBoosts.insert(new BasicDBObject("f","SYW_OWN").append("b",boosts));
		boosts = new BasicDBList();
		boosts.add("BOOST_SYW_OWN_REGRIG_TCOUNT");
		boosts.add("BOOST_SYW_OWN_HA_ALL_TCOUNT");
		feedBoosts.insert(new BasicDBObject("f","SYW_WANT").append("b",boosts));
		
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        ProcessSYWInteractions boltUnderTest = new ProcessSYWInteractions(System.getProperty(MongoNameConstants.IS_PROD));
        TopologyContext context = new MockTopologyContext();
        boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
        String lId = "do0b7SN1eER9shCSj0DX+eSGag=";
		String interactionType = "AddToCatalog";
		String interactionString = "{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":280987671,\"EntityType\":\"Product\"},{\"Id\":15009844,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}";
	
        Tuple tuple = StormTestUtils.mockInteractionTuple(lId, interactionString, interactionType);
        
        boltUnderTest.execute(tuple);
       
        List<Object> outputTupleP = outputCollector.getTuple().get("persist_stream");
        System.out.println(outputCollector.getTuple().get("score_stream"));
        /*[null, {"BOOST_SYW_OWN_HA_ALL_TCOUNT":"{\"current\":[\"02280322000P\"]}","BOOST_SYW_OWN_REGRIG_TCOUNT":"{\"current\":[\"02280322000P\"]}"}, SYW_WANT]*/
        Assert.assertEquals(lId, outputTupleP.get(0));
        Assert.assertEquals("SYW_WANT", outputTupleP.get(2));
        Assert.assertEquals("{\"BOOST_SYW_OWN_HA_ALL_TCOUNT\":\"{\\\"current\\\":[\\\""+pid+"\\\"]}\","
        		+ "\"BOOST_SYW_OWN_REGRIG_TCOUNT\":\"{\\\"current\\\":[\\\""+pid+"\\\"]}\"}",outputTupleP.get(1));
        
        
        List<Object> outputTupleS = outputCollector.getTuple().get("score_stream");
        Assert.assertEquals(lId, outputTupleS.get(0));
        Assert.assertEquals("SYW_WANT", outputTupleS.get(2));
        Assert.assertEquals("{\"BOOST_SYW_OWN_HA_ALL_TCOUNT\":\"0.0189524\"}",outputTupleS.get(1));

	}
	
	@AfterClass
	public static void cleanUp(){
		/*if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());*/
		
		SystemPropertyUtility.dropDatabase();
	}
	
}
