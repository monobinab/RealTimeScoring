package analytics.bolt;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.StormTestUtils;
import analytics.util.FakeMongoStaticCollection;
import analytics.util.JsonUtils;
import analytics.util.SystemPropertyUtility;
import analytics.util.jedis.JedisFactoryStubImpl;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

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
	
	//a positive case
	@SuppressWarnings("unchecked")
	@Test
	public void strategyScoringBoltTest() throws ParseException {
		
		String l_id = "testingLid";
		//fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("16", 1));
	
		//fake changedMemberVariables collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("16", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"16",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
	
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 0000, "0.0.0.0", 0000 );
	
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());
		boltUnderTest.setTestMode(Boolean.TRUE);
		
		Map<String, Object> newChangesVarValuemap = new HashMap<String, Object>();
		newChangesVarValuemap.put("S_SRS_VAR", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(newChangesVarValuemap);
		Tuple tuple = StormTestUtils.mockTuple(l_id, varObjString, "TestingTelluride", "testingLoyaltyId");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		List<Object> outputTuple = outputCollector.getTuple().get("score_stream");
	
		
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject changedMemScoreObj = null;
		DBCursor updatedMemScoreColl = changedMemberScoreColl.find();
		while(updatedMemScoreColl.hasNext()){
			changedMemScoreObj = updatedMemScoreColl.next();
			HashMap<String, ChangedMemberScore> changedMemScores70Updated = (HashMap<String, ChangedMemberScore>) changedMemScoreObj
					.get("70");
			// testing the updated changedMemberScore collection
			Assert.assertEquals("testingLid", changedMemScoreObj.get("l_id"));
			Assert.assertEquals(0.9999999847700205, changedMemScores70Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores70Updated.get("f"));
			Assert.assertEquals("TestingTelluride", changedMemScores70Updated.get("c"));
		}
		
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj = (DBObject) changedMemVars.get("16");
		Assert.assertEquals(13, varObj.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), varObj.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj.get("f"));
		
			
		//testing the tuple emitted in outputcollector 
		Assert.assertEquals(l_id, outputTuple.get(0));
		Assert.assertEquals(0.9999999847700205, outputTuple.get(1));
		Assert.assertEquals("70", outputTuple.get(2));
		Assert.assertEquals("TestingTelluride", outputTuple.get(3));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), outputTuple.get(5));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), outputTuple.get(6));
		
		//testing the fake redis for TI_POS
		boltUnderTest.getFakeJedis().hgetAll("RTS:Telluride:"+l_id);
		//Assert.assertEquals(0.9999999847700205, actualRedisMap.get("70"));
	}
	
	@Test
	public void strategyScoringBoltWithNoMemberFoundTest() throws ParseException {
		
		String l_id = "testingLid2";
		//fake memberVariables collection 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "testingLid").append("15", 1));
		
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
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 6379, "0.0.0.0", 6379 );
		boltUnderTest.setTestMode(Boolean.TRUE);
	
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("VARIABLE4", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid2", varObjString, "TestingTelluride", "testingLoyaltyId2");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		Assert.assertEquals(new HashMap<String, List<Object>>(), outputCollector.getTuple());
	}

}