/*package analytics.bolt;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	
	
	 * If there is no incoming variables from the parsing bolt, i.e. newChangesVarValueMap is empty
	 * strategyBolt will ack the input, thereby output collector will be empty
	 
	@Test
	public void strategyScoringBoltWithEmptyNewChangesVarValueMapTest() throws ParseException {
			
		Map<String, Object> map = new HashMap<String, Object>();
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid_1", varObjString, "testingTopology", "testingLoyaltyId_1");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 6379, "0.0.0.0", 6379 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		Assert.assertEquals(new HashMap<String, List<Object>>(), outputCollector.getTuple());
	}
	
	
	 * If all incoming variables are not of RTS interest (with NONE strategy), there won't be any models to be scored
	 * so, null object will be returned from SS and the input will be acked by the strategybolt
	 * output collector will be empty
	 
	@Test
	public void strategyScoringBoltWithAllIncomingVarsOfNONEStrategyTest() throws ParseException {
		
		String l_id = "testingLid_2";
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
			
		Map<String, Object> newChangesVarValuemap = new HashMap<String, Object>();
		newChangesVarValuemap.put("VARIABLE12", "1.0");
		newChangesVarValuemap.put("VARIABLE40", "1.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(newChangesVarValuemap);
		Tuple tuple = StormTestUtils.mockTuple(l_id, varObjString, "testingTopology_2", "testingLoyaltyId_2");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		
		Assert.assertEquals(new HashMap<String, List<Object>>(), outputCollector.getTuple());
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
			
		Map<String, Object> newChangesVarValuemap = new HashMap<String, Object>();
		newChangesVarValuemap.put("S_SRS_VAR", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(newChangesVarValuemap);
		Tuple tuple = StormTestUtils.mockTuple(l_id, varObjString, "testingTopology", "testingLoyaltyId");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
	
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject changedMemScoreObj = changedMemberScoreColl.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScores70Updated = (HashMap<String, ChangedMemberScore>) changedMemScoreObj.get("70");
			// testing the updated changedMemberScore collection
			Assert.assertEquals("testingLid", changedMemScoreObj.get("l_id"));
			Assert.assertEquals(0.9999999847700205, changedMemScores70Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores70Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores70Updated.get("c"));
				
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj = (DBObject) changedMemVars.get("16");
		Assert.assertEquals(13, varObj.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), varObj.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj.get("f"));
		
			
		//testing the tuple emitted in score_stream of the outputcollector (emitted to Logging Bolt) 
		List<Object> outputTupleScoreStream = outputCollector.getTuple().get("score_stream");
		List<ChangedMemberScore> changedMemberScores = (List<ChangedMemberScore>)outputTupleScoreStream.get(0);
		Assert.assertEquals(l_id, changedMemberScores.get(0).getlId());
		Assert.assertEquals(0.9999999847700205, changedMemberScores.get(0).getScore());
		Assert.assertEquals("70", changedMemberScores.get(0).getModelId());
		Assert.assertEquals("testingTopology", changedMemberScores.get(0).getSource());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMaxDate());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMinDate());
		
		//testing the tuple emitted in kafka_stream of the outputcollector (emitted to kafka for TEC to listen to) 
		//lyl_id_no+"~"+topologyName
		List<Object> outputTupleKafkaStream = outputCollector.getTuple().get("kafka_stream");
		Assert.assertEquals("testingLoyaltyId~testingTopology", outputTupleKafkaStream.get(0));
		
		//testing the fake redis for TI_POS
		Jedis fakeJedis = boltUnderTest.getJedisInterface().createJedis("0.0.0.0", 0000);
		Map<String, String> actualRedisMap = fakeJedis.hgetAll("RTS:Telluride:"+l_id);
		Assert.assertEquals("0.9999999847700205", actualRedisMap.get("70"));
		
		//testing the fake redis for unknown occasion
		Set<String> keys = fakeJedis.keys("Unknown");
		Assert.assertTrue(keys.contains("Unknown:testingLoyaltyId"));
		
		//clearing the fake redis
		fakeJedis.del("Unknown:testingLoyaltyId");
		fakeJedis.del("RTS:Telluride:"+l_id);
		
		//clearing the fake mongo
		changedMemberScoreColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		memVarColl.remove(new BasicDBObject("l_id", l_id));
	}
	
	
	 * If the member does not exist in memberVariables collection, RTS will not score that member
	 
	@Test
	public void strategyScoringBoltWithNoMemberVarsFoundTest() throws ParseException {
		
		String l_id = "testingLid2";
		//fake memberVariables collection, does not contain the memberId "testingLid2" which is tested 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "testingLid").append("16", 1));
		
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
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("S_SRS_VAR", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid2", varObjString, "testingTopology", "testingLoyaltyId2");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 6379, "0.0.0.0", 6379 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		Assert.assertEquals(new HashMap<String, List<Object>>(), outputCollector.getTuple());
	}
	
	
	 * If the member exists but with no vars of interest, he will be scored by RTS, if he has non-expired vars of interest (in changedMemVars collection)
	 
	@SuppressWarnings("unchecked")
	@Test
	public void strategyScoringBoltMemberFoundWithNoVarsOfInterestTest() throws ParseException {
		
		String l_id = "testingLid3";
		//fake memberVariables collection, does not contain the memberId in testing 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1));
		
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
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("S_SRS_VAR", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid3", varObjString, "testingTopology", "testingLoyaltyId3");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 0000, "0.0.0.0", 0000 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject changedMemScoreObj = null;
		DBObject updatedMemScoreObj = changedMemberScoreColl.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScores70Updated = (HashMap<String, ChangedMemberScore>) updatedMemScoreObj
					.get("70");
			// testing the updated changedMemberScore collection
			Assert.assertEquals(0.9999999847700205, changedMemScores70Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores70Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores70Updated.get("c"));
			
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj = (DBObject) changedMemVars.get("16");
		Assert.assertEquals(13, varObj.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), varObj.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj.get("f"));
		
			
		//testing the tuple emitted in score_stream of the outputcollector (emitted to Logging Bolt) 
		List<Object> outputTupleScoreStream = outputCollector.getTuple().get("score_stream");
		List<ChangedMemberScore> changedMemberScores = (List<ChangedMemberScore>)outputTupleScoreStream.get(0);
		Assert.assertEquals(l_id, changedMemberScores.get(0).getlId());
		Assert.assertEquals(0.9999999847700205, changedMemberScores.get(0).getScore());
		Assert.assertEquals("70", changedMemberScores.get(0).getModelId());
		Assert.assertEquals("testingTopology", changedMemberScores.get(0).getSource());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMaxDate());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMinDate());
		
		//testing the tuple emitted in kafka_stream of the outputcollector (emitted to kafka for TEC to listen to) 
		//lyl_id_no+"~"+topologyName
		List<Object> outputTupleKafkaStream = outputCollector.getTuple().get("kafka_stream");
		Assert.assertEquals("testingLoyaltyId3~testingTopology", outputTupleKafkaStream.get(0));
		
		//testing the fake redis for TI_POS
		Jedis fakeJedis = boltUnderTest.getJedisInterface().createJedis("0.0.0.0", 0000);
		Map<String, String> actualRedisMap = fakeJedis.hgetAll("RTS:Telluride:"+l_id);
		Assert.assertEquals("0.9999999847700205", actualRedisMap.get("70"));
		
		//testing the fake redis for unknown occasion
		Set<String> keys = fakeJedis.keys("Unknown");
		Assert.assertTrue(keys.contains("Unknown:testingLoyaltyId3"));
		
		//clearing the fake redis
		fakeJedis.del("Unknown:testingLoyaltyId3");
		fakeJedis.del("RTS:Telluride:"+l_id);
		
		//clearing the fake mongo
		changedMemberScoreColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		memVarColl.remove(new BasicDBObject("l_id", l_id));
	}
	
	
	 * If the incoming variable is a blackout variable, the model associated will be blacked out with a score of 0.0
	 * expiration will be set for 30 days based on blackout strategy
	 
	@SuppressWarnings("unchecked")
	@Test
	public void strategyScoringBoltWithBlackoutIncomingVarTest() throws ParseException {
		
		String l_id = "testingLid4";
		//fake memberVariables collection, does not contain the memberId in testing 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1));
		
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
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("BLACKOUT_VARIABLE", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid4", varObjString, "testingTopology", "testingLoyaltyId4");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 0000, "0.0.0.0", 0000 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject changedMemScoreObj = changedMemberScoreColl.findOne(new BasicDBObject("l_id", l_id));
	
			HashMap<String, ChangedMemberScore> changedMemScores46Updated = (HashMap<String, ChangedMemberScore>) changedMemScoreObj
					.get("46");
			// testing the updated changedMemberScore collection
			Assert.assertEquals(0.0, changedMemScores46Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemScores46Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemScores46Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores46Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores46Updated.get("c"));
		
		
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj = (DBObject) changedMemVars.get("11");
		Assert.assertEquals(1, varObj.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), varObj.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj.get("f"));
		
			
		//testing the tuple emitted in score_stream of the outputcollector (emitted to Logging Bolt) 
		List<Object> outputTupleScoreStream = outputCollector.getTuple().get("score_stream");
		List<ChangedMemberScore> changedMemberScores = (List<ChangedMemberScore>)outputTupleScoreStream.get(0);
		Assert.assertEquals(l_id, changedMemberScores.get(0).getlId());
		Assert.assertEquals(0.0, changedMemberScores.get(0).getScore());
		Assert.assertEquals("46", changedMemberScores.get(0).getModelId());
		Assert.assertEquals("testingTopology", changedMemberScores.get(0).getSource());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemberScores.get(0).getMaxDate());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemberScores.get(0).getMinDate());
		
		//testing the tuple emitted in kafka_stream of the outputcollector (emitted to kafka for TEC to listen to) 
		//lyl_id_no+"~"+topologyName
		List<Object> outputTupleKafkaStream = outputCollector.getTuple().get("kafka_stream");
		Assert.assertEquals("testingLoyaltyId4~testingTopology", outputTupleKafkaStream.get(0));
		
		//testing the fake redis for TI_POS
		Jedis fakeJedis = boltUnderTest.getJedisInterface().createJedis("0.0.0.0", 0000);
		Map<String, String> actualRedisMap = fakeJedis.hgetAll("RTS:Telluride:"+l_id);
		Assert.assertEquals("0.0", actualRedisMap.get("46"));
		
		//testing the fake redis for unknown occasion
		Set<String> keys = fakeJedis.keys("Unknown");
		Assert.assertTrue(keys.contains("Unknown:testingLoyaltyId4"));
		
		//clearing the fake redis
		fakeJedis.del("Unknown:testingLoyaltyId4");
		fakeJedis.del("RTS:Telluride:"+l_id);
		
		//clearing the fake mongo
		changedMemberScoreColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		memVarColl.remove(new BasicDBObject("l_id", l_id));
	}
		
	
	 * If the incoming variable is not a blackout variable, but the model affected by the inc var has a blackout var
	 * which is unexpired for this member based on previous activity
	 * then the model will be blacked out with a score of 0.0 and expiration will be set for 30 days based on blackout strategy
	 * here model 75 has VID 17 associated with it, so will be blacked out, but model 70 which has only S_SRS_VAR association, will be scored with value
	 
	@SuppressWarnings("unchecked")
	@Test
	public void strategyScoringBoltWithNoIncomingBlackoutWithUnexpiredBlackoutChangedMemVarTest() throws ParseException {
		
		String l_id = "testingLid5";
		//fake memberVariables collection, does not contain the memberId in testing 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1));
		
		//fake changedMemberVariables collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("18", 1,
				simpleDateFormat.parse(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate())),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"18",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("S_SRS_VAR2", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid5", varObjString, "testingTopology", "testingLoyaltyId5");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 0000, "0.0.0.0", 0000 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject updatedMemScoreObject = changedMemberScoreColl.findOne(new BasicDBObject("l_id", l_id));
		
			HashMap<String, ChangedMemberScore> changedMemScores75Updated = (HashMap<String, ChangedMemberScore>) updatedMemScoreObject
					.get("75");
			// testing the updated changedMemberScore collection
			Assert.assertEquals(0.0, changedMemScores75Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores75Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemScores75Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores75Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores75Updated.get("c"));
					
			HashMap<String, ChangedMemberScore> changedMemScores70Updated = (HashMap<String, ChangedMemberScore>) updatedMemScoreObject
					.get("75");
			// testing the updated changedMemberScore collection
			Assert.assertEquals(0.0, changedMemScores70Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemScores70Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores70Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores70Updated.get("c"));
		
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj = (DBObject) changedMemVars.get("18");
		Assert.assertEquals(1, varObj.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), varObj.get("e"));
		Assert.assertEquals("2014-09-01", varObj.get("f"));
		
		DBObject varObj2 = (DBObject) changedMemVars.get("17");
		Assert.assertEquals(1, varObj2.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), varObj2.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj2.get("f"));
		
			
		//testing the tuple emitted in score_stream of the outputcollector (emitted to Logging Bolt) 
		List<Object> outputTupleScoreStream = outputCollector.getTuple().get("score_stream");
		List<ChangedMemberScore> changedMemberScores = (List<ChangedMemberScore>)outputTupleScoreStream.get(0);
		Assert.assertEquals(l_id, changedMemberScores.get(0).getlId());
		Assert.assertEquals(0.0, changedMemberScores.get(0).getScore());
		Assert.assertEquals("75", changedMemberScores.get(0).getModelId());
		Assert.assertEquals("testingTopology", changedMemberScores.get(0).getSource());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMinDate());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(730).toDateMidnight().toDate()), changedMemberScores.get(0).getMaxDate());
		
		//testing the tuple emitted in kafka_stream of the outputcollector (emitted to kafka for TEC to listen to) 
		//lyl_id_no+"~"+topologyName
		List<Object> outputTupleKafkaStream = outputCollector.getTuple().get("kafka_stream");
		Assert.assertEquals("testingLoyaltyId5~testingTopology", outputTupleKafkaStream.get(0));
		
		//testing the fake redis for TI_POS
		Jedis fakeJedis = boltUnderTest.getJedisInterface().createJedis("0.0.0.0", 0000);
		Map<String, String> actualRedisMap = fakeJedis.hgetAll("RTS:Telluride:"+l_id);
		Assert.assertEquals("0.0", actualRedisMap.get("75"));
			
		//testing the fake redis for unknown occasion
		Set<String> keys = fakeJedis.keys("Unknown");
		Assert.assertTrue(keys.contains("Unknown:testingLoyaltyId5"));
		
		//clearing the fake redis
		fakeJedis.del("Unknown:testingLoyaltyId5");
		fakeJedis.del("RTS:Telluride:"+l_id);
		
		//clearing the fake mongo
		changedMemberScoreColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		memVarColl.remove(new BasicDBObject("l_id", l_id));
	}
		
	
	 * If a member has an non-expired change var based on previous activity, which is removed from RTS system (i.e. from variables and modelVariables collection)
	 * it won't be populated in changedMemVars at all
	 * hence, it will be skipped in scoring, expiration dates etc. Here, VID 19 is not in our collections, so only the incoming S_SRS_VAR will be used
	 * Got NPE in PRODUCTION and the null check was included
	 
	@SuppressWarnings("unchecked")
	@Test
	public void strategyScoringBoltWithUnexpChangedMemVarNotInRTSTest() throws ParseException {
		
		String l_id = "testingLid6";
		//fake memberVariables collection, does not contain the memberId in testing 
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1));
		
		//fake changedMemberVariables collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("19", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"19",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("S_SRS_VAR", "102.0");
		String varObjString = (String) JsonUtils.createJsonFromStringObjectMap(map);
		Tuple tuple = StormTestUtils.mockTuple("testingLid6", varObjString, "testingTopology", "testingLoyaltyId6");
		
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
			
		StrategyScoringBolt boltUnderTest = new StrategyScoringBolt(System.getProperty("rtseprod"), "0.0.0.0", 0000, "0.0.0.0", 0000 );
		boltUnderTest.setJedisInterface(new JedisFactoryStubImpl());	
		boltUnderTest.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		boltUnderTest.execute(tuple);
		
		//fake changedMemberScores collection with no record in it
		DBCollection changedMemberScoreColl = db.getCollection("changedMemberScores");
		DBObject changedMemScoreObj = changedMemberScoreColl.findOne(new BasicDBObject("l_id", l_id));
			
			HashMap<String, ChangedMemberScore> changedMemScores70Updated = (HashMap<String, ChangedMemberScore>) changedMemScoreObj
					.get("70");
			// testing the updated changedMemberScore collection
			Assert.assertEquals("testingLid6", changedMemScoreObj.get("l_id"));
			Assert.assertEquals(0.9975273768433652, changedMemScores70Updated.get("s"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("minEx"));
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemScores70Updated.get("maxEx"));
			Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScores70Updated.get("f"));
			Assert.assertEquals("testingTopology", changedMemScores70Updated.get("c"));
			
		//testing the updated changedMemberVariables collection
		DBObject changedMemVars = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
		DBObject varObj11 = (DBObject) changedMemVars.get("16");
		Assert.assertEquals(1, varObj11.get("v"));
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), varObj11.get("e"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), varObj11.get("f"));
	
		//testing the tuple emitted in score_stream of the outputcollector (emitted to Logging Bolt) 
		List<Object> outputTupleScoreStream = outputCollector.getTuple().get("score_stream");
		List<ChangedMemberScore> changedMemberScores = (List<ChangedMemberScore>)outputTupleScoreStream.get(0);
		Assert.assertEquals(l_id, changedMemberScores.get(0).getlId());
		Assert.assertEquals(0.9975273768433652, changedMemberScores.get(0).getScore());
		Assert.assertEquals("70", changedMemberScores.get(0).getModelId());
		Assert.assertEquals("testingTopology", changedMemberScores.get(0).getSource());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMaxDate());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemberScores.get(0).getMinDate());
		
		//testing the tuple emitted in kafka_stream of the outputcollector (emitted to kafka for TEC to listen to) 
		//lyl_id_no+"~"+topologyName
		List<Object> outputTupleKafkaStream = outputCollector.getTuple().get("kafka_stream");
		Assert.assertEquals("testingLoyaltyId6~testingTopology", outputTupleKafkaStream.get(0));
		
		//testing the fake redis for TI_POS
		Jedis fakeJedis = boltUnderTest.getJedisInterface().createJedis("0.0.0.0", 0000);
		Map<String, String> actualRedisMap = fakeJedis.hgetAll("RTS:Telluride:"+l_id);
		Assert.assertEquals("0.9975273768433652", actualRedisMap.get("70"));
		
		//testing the fake redis for unknown occasion
		Set<String> keys = fakeJedis.keys("Unknown");
		Assert.assertTrue(keys.contains("Unknown:testingLoyaltyId6"));
		
		//clearing the fake redis
		fakeJedis.del("Unknown:testingLoyaltyId6");
		fakeJedis.del("RTS:Telluride:"+l_id);
		
		//clearing the fake mongo
		changedMemberScoreColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		memVarColl.remove(new BasicDBObject("l_id", l_id));
	}
	
}*/