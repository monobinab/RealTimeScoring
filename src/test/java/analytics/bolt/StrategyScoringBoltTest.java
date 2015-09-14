package analytics.bolt;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.FakeMongoStaticCollection;
import analytics.util.JsonUtils;
import analytics.util.StubJedisFactory;
import analytics.util.SystemPropertyUtility;
import analytics.util.jedis.JedisFactoryStubImpl;
import analytics.util.objects.Change;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

import com.fiftyonred.mock_jedis.MockJedis;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class StrategyScoringBoltTest {
	@SuppressWarnings("unused")
	private static FakeMongoStaticCollection fakeMongoStaticCollection;
	private static DB db;
	
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
			
		SystemPropertyUtility.setSystemProperty();
		fakeMongoStaticCollection = new FakeMongoStaticCollection();
		db = SystemPropertyUtility.getDb();
		
	}
	
	@Test
	public void test() throws ParseException {
		
		String l_id = "testingLid";
		//fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("15", 1));
	
		//fake changedMemberVariables collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("15", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"15",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		//fake changedMemberVariables collection
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 6379, "0.0.0.0", 6379 );
	
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("BOOST_DC_VAR", "10000.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid", varObjString, "DC", "testingLoyaltyId");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		List<Object> outputTuple = outputCollector.getTuple().get("score_stream");
	
		DBObject changedMemScoreObj = null;
		DBCursor updatedMemScoreColl = changedMemberScoreColl.find();
		while(updatedMemScoreColl.hasNext()){
			changedMemScoreObj = updatedMemScoreColl.next();
		}
		
		//updated collections testing
		Assert.assertEquals("testingLid", changedMemScoreObj.get("l_id"));
		
		//tuple emitted in outputcollector testing
		Assert.assertEquals(1.0, outputTuple.get(1));
	}

}