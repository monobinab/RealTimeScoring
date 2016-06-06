package browse;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import scala.actors.threadpool.Arrays;
import analytics.MockOutputCollector;
import analytics.MockTopologyContext;
import analytics.bolt.BrowseCountPersistBolt;
import analytics.util.FakeMongoStaticCollection;
import analytics.util.SystemPropertyUtility;
import analytics.util.dao.MemberBrowseDao;
import analytics.util.objects.MemberBrowse;
import backtype.storm.task.TopologyContext;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class BrowseCountPersistboltTest {

	@SuppressWarnings("unused")
	private static FakeMongoStaticCollection fakeMongoStaticCollection;
	private static DB db;
	LocalDate date = new LocalDate(new Date());
	SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	MemberBrowseDao memberBrowseDao = new MemberBrowseDao();
	BrowseCountPersistBolt browseCountPersistBolt = new BrowseCountPersistBolt("test", "testSB", "testingWeb", "testingKafka");
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
			
		SystemPropertyUtility.setSystemProperty();
		fakeMongoStaticCollection = new FakeMongoStaticCollection();
		db = SystemPropertyUtility.getDb();
	}
	
	/*
	 * If the member does not have record at all, incomingBuSubBu will be inserted for today's date
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListNoRecordMbrBrowseTest(){
		
		String l_id = "testingLid";
		String loyaltyId = "testingLoyaltyId";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		
		MemberBrowse memberBrowse = null;
			
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println("checking the memberBrowse before the method call: " + testing);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println("checking the memberBrowse after the method call: " + actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertEquals( new HashSet<String>(){{add("EFGHI7");}}, buSubBuList);
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}

	/*
	 * If the member does not have record at all, incomingBuSubBu will be inserted for today's date
	 * EFGH1 count is == TH, so will be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListNoRecordMbrBrowseAndICEQToTHTest(){
		
		String l_id = "testingLid_1";
		String loyaltyId = "testingLoyaltyId_1";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "4");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println("checking the memberBrowse before the method call: " + testing);
	
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);//getMemberBrowse7dayHistory
			
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println("checking the memberBrowse after the method call: " + actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 4);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertTrue( buSubBuList.contains("EFGHI7"));
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}

	
	/*
	 * If the member does not have record for today, incomingBuSubBu will be inserted for today's date
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListInsertMbrBrowseTodayTest(){
		
		String l_id = "testingLid2";
		String loyaltyId = "testingLoyaltyId2";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
		
		Date dateInColl = date.minusDays(2).toDateMidnight().toDate();
		String dateString = dateFormatter.format(dateInColl);
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		/*Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertEquals(new HashSet<String>(){{add("EFGHI7");}}, buSubBuList);
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with different buSubBu and same feed from incomingBuSubBu for today, incoming buSubBu will be updated for today's date
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodayDiffTagSameFeedTest(){
		
		String l_id = "testingLid3";
		String loyaltyId = "testingLoyaltyId3";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		Map<String, Integer> expectedFeedCountsMap3 =  new HashMap<String, Integer>();
		expectedFeedCountsMap3.put("testSG", 1);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap3);
		Map<String, Integer> expectedFeedCountsMap4 =  new HashMap<String, Integer>();
		expectedFeedCountsMap4.put("testSG", 1);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap4);
		
		//Assertion
		Assert.assertEquals( new HashSet<String>(){{add("EFGHI7");}}, buSubBuList);
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with different buSubBu and same feed from incomingBuSubBu for today, incoming buSubBu will be updated for today's date
	 * Incoming and existing buSubBus are different and PC = TH, so wont be emitted to kafka
	 * incoming buSubBus not = or not > TH, so wont be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodayDiffTagSameFeedPCEQTHAndICLTTHTest(){
		
		String l_id = "testingLid3_1";
		String loyaltyId = "testingLoyaltyId3_1";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 4);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 4);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		Map<String, Integer> expectedFeedCountsMap3 =  new HashMap<String, Integer>();
		expectedFeedCountsMap3.put("testSG", 4);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap3);
		Map<String, Integer> expectedFeedCountsMap4 =  new HashMap<String, Integer>();
		expectedFeedCountsMap4.put("testSG", 4);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap4);
		
		//Assertion
		Assert.assertEquals("list to be emitted to kafka empty as PC=TH", new HashSet<String>(){{add("EFGHI7");}}, buSubBuList);
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with different buSubBu and same feed from incomingBuSubBu for today, incoming buSubBu will be updated for today's date
	 * Incoming and existing buSubBus are different and PC = TH, so wont be emitted to kafka
	 * incoming buSubBus ABCDE > TH, so will be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodayDiffTagSameFeedPCEQTHAndICGTTHTest(){
		
		String l_id = "testingLid3_2";
		String loyaltyId = "testingLoyaltyId3_2";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "5");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 4);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 4);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 5);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap2);
		Map<String, Integer> expectedFeedCountsMap3 =  new HashMap<String, Integer>();
		expectedFeedCountsMap3.put("testSG", 4);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap3);
		Map<String, Integer> expectedFeedCountsMap4 =  new HashMap<String, Integer>();
		expectedFeedCountsMap4.put("testSG", 4);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap4);
		
		//Assertion
	//	Assert.assertEquals( "ABCDE7", buSubBuList.get(0));
		Assert.assertTrue(buSubBuList.contains("ABCDE7"));
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with same buSubBu and same feed for today, incomingBuSubBu will be updated for today's date
	 * counts will be added up for each buSubBu
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodaySameTagSameFeedTest(){
		
		String l_id = "testingLid4";
		String loyaltyId = "testingLoyaltyId4";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("XYZW1", "1");
		incomingBuSubBuMap.put("12345", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 2);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 3);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}

	/*
	 * If the member has record with same buSubBu and same feed for today, incomingBuSubBu will be updated for today's date
	 * counts will be added up for each buSubBu
	 * 12345's PC+IC=4, will be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodaySameTagSameFeedICAndPCEQTHTest(){
		
		String l_id = "testingLid4_1";
		String loyaltyId = "testingLoyaltyId4_1";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("XYZW1", "1");
		incomingBuSubBuMap.put("12345", "3");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testSG", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testSG", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
			
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 2);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 4);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		
		//Assertion
		//Assert.assertEquals( "123457", buSubBuList.get(0));
		Assert.assertTrue(buSubBuList.contains("123457"));
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	/*
	 * If the member has record with same buSubBu and diff feed for today from incomingBuSubBu, incomingBuSubBu will be updated for today's date
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodaySameTagDiffFeedTest(){
		
		String l_id = "testingLid6";
		String loyaltyId = "testingLoyaltyId6";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("XYZW1", "1");
		incomingBuSubBuMap.put("12345", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testPR", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		
	/*	MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testPR", 1);
		Map<String, Integer> feedCountMap2 = new HashMap<String, Integer>();
		feedCountMap2.put("testPR", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap2);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedFeedCountsMap.put("testPR", 1);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedFeedCountsMap2.put("testPR", 1);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with same buSubBu and diff feed for today from incomingBuSubBu, incomingBuSubBu will be updated for today's date
	 * 12345 PC<TH and PC+IC>TH, will be emitted to kafka, but XYZW1 PC=Th, will not be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodaySameTagDiffFeedPCICGTTHTest(){
		
		String l_id = "testingLid6_1";
		String loyaltyId = "testingLoyaltyId6_1";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("XYZW1", "1");
		incomingBuSubBuMap.put("12345", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testPR", 3);
		BasicDBObject feedCountDbObj2 = new BasicDBObject();
		feedCountDbObj2.append("testIS", 4);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj2);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
	/*	MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testPR", 3);
		Map<String, Integer> feedCountMap2 = new HashMap<String, Integer>();
		feedCountMap2.put("testIS", 4);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap2);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedFeedCountsMap.put("testIS", 4);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedFeedCountsMap2.put("testPR", 3);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		
		//Assertion
		//Assert.assertEquals("123457", buSubBuList.get(0));
		Assert.assertTrue(buSubBuList.isEmpty());
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	/*
	 * If the member has record with same buSubBu and diff feed for today from incomingBuSubBu, incomingBuSubBu will be updated for today's date
	 * 12345 and XYZW1 PC<TH and PC+IC>TH, so both will be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodaySameTagDiffFeedPCICGTTHTest2(){
		
		String l_id = "testingLid6_2";
		String loyaltyId = "testingLoyaltyId6_2";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("XYZW1", "1");
		incomingBuSubBuMap.put("12345", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testPR", 3);
		BasicDBObject feedCountDbObj2 = new BasicDBObject();
		feedCountDbObj2.append("testIS", 3);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj2);
		browsebuSubBuDbObj.append("12345", feedCountDbObj);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testPR", 3);
		Map<String, Integer> feedCountMap2 = new HashMap<String, Integer>();
		feedCountMap2.put("testIS", 3);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap2);
		buSpeMap.put("12345", feedCountMap);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		Set<String> buSubBuList = browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testSG", 1);
		expectedFeedCountsMap.put("testIS", 3);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testSG", 2);
		expectedFeedCountsMap2.put("testPR", 3);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		
		//Assertion
		Assert.assertTrue(  buSubBuList.isEmpty());
		Assert.assertTrue(buSubBuList.isEmpty());
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	/*
	 * If the member has record with diff buSubBu and diff feed for today from incomingBuSubBu, incomingBuSubBu will be updated for today's date
	 * PC+IC < TH for all buSubBu, will not be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodayDiffTagDiffFeedTest(){
		
		String l_id = "testingLid5";
		String loyaltyId = "testingLoyaltyId5";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "2");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testPR", 1);
		BasicDBObject feedCountDbObj2 = new BasicDBObject();
		feedCountDbObj2.append("testIS", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj2);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
		/*MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testPR", 1);
		Map<String, Integer> feedCountMap2 = new HashMap<String, Integer>();
		feedCountMap2.put("testIS", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap2);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testPR", 1);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testIS", 1);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		Map<String, Integer> expectedFeedCountsMap3 =  new HashMap<String, Integer>();
		expectedFeedCountsMap3.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap3);
		Map<String, Integer> expectedFeedCountsMap4 =  new HashMap<String, Integer>();
		expectedFeedCountsMap4.put("testSG", 2);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap4);
		
		//Assertion
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
	
	
	/*
	 * If the member has record with diff buSubBu and diff feed for today from incomingBuSubBu, incomingBuSubBu will be updated for today's date
	 * IC = TH for ABCDE, will be emitted to kafka
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void updateMemberBrowseAndKafkaListUpdateMbrBrowseTodayDiffTagDiffFeedINEQTHTest(){
		
		String l_id = "testingLid5_1";
		String loyaltyId = "testingLoyaltyId5_1";
		TopologyContext context = new MockTopologyContext();
		MockOutputCollector outputCollector = new MockOutputCollector(null);
		browseCountPersistBolt.prepare(SystemPropertyUtility.getStormConf(), context, outputCollector);
		
		//prepared incomingBuSubBuMap
		Map<String, String> incomingBuSubBuMap = new HashMap<String, String>();
		incomingBuSubBuMap.put("ABCDE", "1");
		incomingBuSubBuMap.put("EFGHI", "4");
		
		//fake memberBrowse collection
		DBCollection memberBrowseColl = db.getCollection("memberBrowse");
		BasicDBObject feedCountDbObj = new BasicDBObject();
		feedCountDbObj.append("testPR", 1);
		BasicDBObject feedCountDbObj2 = new BasicDBObject();
		feedCountDbObj2.append("testIS", 1);
		
		BasicDBObject browsebuSubBuDbObj = new BasicDBObject();
		browsebuSubBuDbObj.append("XYZW1", feedCountDbObj);
		browsebuSubBuDbObj.append("12345", feedCountDbObj2);
	
		String dateString = dateFormatter.format(new Date());
	
		BasicDBObject mainDbObj = new BasicDBObject();
		mainDbObj.append("l_id", l_id).append(dateString, browsebuSubBuDbObj);
		
		memberBrowseColl.insert(mainDbObj);
		
		DBObject testing = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));
		System.out.println(testing);
		
	/*	MemberBrowse memberBrowse = new MemberBrowse();
		Map<String, Integer> feedCountMap = new HashMap<String, Integer>();
		feedCountMap.put("testPR", 1);
		Map<String, Integer> feedCountMap2 = new HashMap<String, Integer>();
		feedCountMap2.put("testIS", 1);
		Map<String, Map<String, Integer>> buSpeMap = new HashMap<String, Map<String,Integer>>();
		buSpeMap.put("XYZW1", feedCountMap);
		buSpeMap.put("12345", feedCountMap2);
		Map<String, Map<String, Map<String, Integer>>> dateSpecificBuSubBu = new HashMap<String, Map<String,Map<String,Integer>>>();
		dateSpecificBuSubBu.put(dateString, buSpeMap);
		memberBrowse.setDateSpecificBuSubBu(dateSpecificBuSubBu);
		memberBrowse.setL_id(l_id);*/
		
		MemberBrowse memberBrowse = memberBrowseDao.getMemberBrowse7dayHistory(l_id);
		
		browseCountPersistBolt.updateMemberBrowseAndKafkaList(loyaltyId, l_id, incomingBuSubBuMap, memberBrowse);
		
		DBObject mbrBrowseDbObj = memberBrowseColl.findOne(new BasicDBObject("l_id", l_id));

		DBObject todayDBObj = (DBObject) mbrBrowseDbObj.get(dateFormatter.format(new Date()));
		System.out.println(todayDBObj.keySet());
		Map<String, Map<String, Integer>> actualBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		for(String buSubBu: todayDBObj.keySet()){
			Map<String, Integer> actualFeedCountsMap = new HashMap<String, Integer>();
			actualFeedCountsMap = (HashMap<String, Integer>) todayDBObj.get(buSubBu);
			actualBuSubBuMap.put(buSubBu, actualFeedCountsMap);
		}
		
		System.out.println(actualBuSubBuMap);
		
		Map<String, Map<String, Integer>> expectedBuSubBuMap = new HashMap<String, Map<String,Integer>>();
		Map<String, Integer> expectedFeedCountsMap =  new HashMap<String, Integer>();
		expectedFeedCountsMap.put("testPR", 1);
		expectedBuSubBuMap.put("XYZW1", expectedFeedCountsMap);
		Map<String, Integer> expectedFeedCountsMap2 =  new HashMap<String, Integer>();
		expectedFeedCountsMap2.put("testIS", 1);
		expectedBuSubBuMap.put("12345", expectedFeedCountsMap2);
		Map<String, Integer> expectedFeedCountsMap3 =  new HashMap<String, Integer>();
		expectedFeedCountsMap3.put("testSG", 1);
		expectedBuSubBuMap.put("ABCDE", expectedFeedCountsMap3);
		Map<String, Integer> expectedFeedCountsMap4 =  new HashMap<String, Integer>();
		expectedFeedCountsMap4.put("testSG", 4);
		expectedBuSubBuMap.put("EFGHI", expectedFeedCountsMap4);
		
		//Assertion
		Assert.assertEquals(expectedBuSubBuMap.keySet(), actualBuSubBuMap.keySet());
		
		//Iterating through the expectedBuSubBuMap and checking with actualBuSubBuMap
		for(String buSubBu : expectedBuSubBuMap.keySet()){
			Map<String, Integer> feedCountsMap = expectedBuSubBuMap.get(buSubBu);
			for(String feed :  feedCountsMap.keySet()){
				Assert.assertEquals(feedCountsMap.get(feed), actualBuSubBuMap.get(buSubBu).get(feed));
			}
		}
	}
}
