package analytics.bolt;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.StormTestUtils;
import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.SywApiCalls;
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
	@BeforeClass
	public static void initializeFakeMongo(){
		System.setProperty("rtseprod", "test");	
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));			
	}
	@Test
	public void onlyCertainCatalogTypesAreProcessed(){		
		MockOutputCollector outputCollector = new MockOutputCollector(null);
        ProcessSYWInteractions boltUnderTest = new ProcessSYWInteractions();
       
        boltUnderTest.prepare(null, null, outputCollector);
        String lId = "dxo0b7SN1eER9shCSj0DX+eSGag=";
		String interactionType = "AddToCatalog";
		String interactionString = "{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":184008680,\"EntityType\":\"Product\"},{\"Id\":8353466,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}";
	
        //source: facebookSpout:3, stream: default, id: {}, [8/1/2014 7:07,1123404212,[0.0],[0.0],dishwasher]
        Tuple tuple = StormTestUtils.mockInteractionTuple(lId, interactionString, interactionType);
        
        boltUnderTest.execute(tuple);
        
        List<Object> outputTuple = outputCollector.getTuple();
        Assert.assertNull(outputTuple);
	}
	
	@Test
	public void standardCatalogsAreProcessed() throws ConfigurationException{
		/*
		 *     15009844,    15009912,    16881520 - like, want and own catalog ids
		 */
		DB conn = DBConnection.getDBConnection();
		DBCollection pidDivLn = conn.getCollection("pidDivLn");
		pidDivLn.insert(new BasicDBObject("pid","02280322000P").append("d","046").append("l","04601"));
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
        ProcessSYWInteractions boltUnderTest = new ProcessSYWInteractions();
       
        boltUnderTest.prepare(null, null, outputCollector);
        String lId = "dxo0b7SN1eER9shCSj0DX+eSGag=";
		String interactionType = "AddToCatalog";
		String interactionString = "{\"InteractionId\":\"b7556eb8-e9ca-4e31-accc-4b56b69fcfad\",\"UserId\":6875997,\"UserSearsId\":6875997,\"Entities\":"
        		+ "[{\"Id\":280987671,\"EntityType\":\"Product\"},{\"Id\":15009844,\"EntityType\":\"Catalog\",\"OwnerId\":6875997}],\"InteractionType\":\"AddToCatalog\","
        		+ "\"Time\":\"2014-09-24T13:27:45.3874132Z\",\"Client\":\"Web\"}";
	
        Tuple tuple = StormTestUtils.mockInteractionTuple(lId, interactionString, interactionType);
        
        boltUnderTest.execute(tuple);

        List<Object> outputTuple = outputCollector.getTuple();
        /*[null, {"BOOST_SYW_OWN_HA_ALL_TCOUNT":"{\"current\":[\"02280322000P\"]}","BOOST_SYW_OWN_REGRIG_TCOUNT":"{\"current\":[\"02280322000P\"]}"}, SYW_WANT]*/
        Assert.assertEquals(lId, outputTuple.get(0));
        Assert.assertEquals("SYW_WANT", outputTuple.get(2));
        Assert.assertEquals("{\"BOOST_SYW_OWN_HA_ALL_TCOUNT\":\"{\\\"current\\\":[\\\"02280322000P\\\"]}\","
        		+ "\"BOOST_SYW_OWN_REGRIG_TCOUNT\":\"{\\\"current\\\":[\\\"02280322000P\\\"]}\"}",outputTuple.get(1));
     

	}
	
	
	
}
