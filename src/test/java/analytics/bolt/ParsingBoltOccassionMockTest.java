package analytics.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.DBConnection;
import analytics.util.FakeMongo;
import analytics.util.SystemPropertyUtility;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import com.github.fakemongo.Fongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ParsingBoltOccassionMockTest extends SystemPropertyUtility{
	
	//static Map<String,String> conf;
	//static DB db;
	static ParsingBoltOccassion parsingBoltOccassion;
	static DBCollection tagsMetadaColl;
	static DBCollection tagsVarColl;
	static DBCollection modelPercColl;
	static Map<String, String> stormConf;
	@BeforeClass
	public static void intializeFakeMongo() throws ConfigurationException{
		//System.setProperty("rtseprod", "test");
		
		/*FakeMongo.setDBConn(new Fongo("test db").getDB("test"));	
		db = DBConnection.getDBConnection();*/
		/*stormConf = new HashMap<String, String>();
		stormConf.put("nimbus.host", "test");*/
		//get the fakMongo collections from ParsingBotlOccassionDaoTest
		SystemPropertyUtility.setSystemProperty();
		ParsingBoltOccassionFakeMonogColl.fakeMongoColl();
		tagsMetadaColl = ParsingBoltOccassionFakeMonogColl.getTagMetadataColl();
		tagsVarColl = ParsingBoltOccassionFakeMonogColl.getTagVariableColl();
		modelPercColl = ParsingBoltOccassionFakeMonogColl.getModelPercColl();
	}
	
	@Test
	public void parsingBoltEmissionTest(){
		ParsingBoltOccassion boltUnderTest = new ParsingBoltOccassion(System.getProperty("rtseprod"));
		String input = "{\"lyl_id_no\":\"7081000000000000\",\"tags\":[\"HACKS2010\",\"HALAS2010\",\"HARFS2010\"]}";
		Tuple tuple = StormTestUtils.mockTuple(input, "PurchaseOccassion");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		boltUnderTest.prepare(stormConf, context, outputCollector);
		System.out.println(tuple.getStringByField("message"));
		boltUnderTest.execute(tuple);
		List<Object> outputTuple = outputCollector.getTuple().get("main");
		 
		 String l_id_expected = "iFTsBvgexZasfSxbq2nOtwAj4bc=";
		 String var_Map_Expected = "{\"BOOST_PO_HA_LA_TEST\":\"0.11\",\"BOOST_PO_HA_COOK_TEST\":\"0.11\",\"BOOST_PO_HA_REF_TEST\":\"0.11\"}";
		 
		 Assert.assertEquals(l_id_expected, outputTuple.get(0));
		 Assert.assertEquals(var_Map_Expected, outputTuple.get(1));
	}
	
	@AfterClass
	public static void tearDown(){
		/*if(ParsingBoltOccassionFakeMonogColl.db.toString().equalsIgnoreCase("FongoDB.test"))
			ParsingBoltOccassionFakeMonogColl.db.dropDatabase();
		  else
		   Assert.fail("Something went wrong. Tests connected to " + ParsingBoltOccassionFakeMonogColl.db.toString());*/
		SystemPropertyUtility.dropDatabase();
	}
}
