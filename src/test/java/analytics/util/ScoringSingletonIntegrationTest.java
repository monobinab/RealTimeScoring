package analytics.util;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.MemberRTSChanges;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ScoringSingletonIntegrationTest {
	
	private static ScoringSingleton scoringSingletonObj;
	private static DB db;

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
			
		SystemPropertyUtility.setSystemProperty();
		db = SystemPropertyUtility.getDb();
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		
		//fake variables collection
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "variable1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "variable2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "variable3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "variable4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "variable5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "variable6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "variable7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "Boost_Syw_variable7").append("VID", 71).append("strategy","StrategySywTotalCounts"));
		varColl.insert(new BasicDBObject("name", "variable8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "variable9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "variable10").append("VID", 10).append("strategy","StrategySumSales"));
		
		//fake modelVariables collection
		DBCollection modeVarColl = db.getCollection("modelVariables");
		BasicDBList dbList = new BasicDBList();
		dbList.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		dbList.add(new BasicDBObject("name", "variable10").append("coefficient", 0.05));
		dbList.add(new BasicDBObject("name", "Boost_Syw_variable7").append("coefficient", 0.1).append("intercept", 0.0));
		modeVarColl.insert(new BasicDBObject("modelId", 35).append("modelName", "Model_Name").append("modelDescription", "Apparel").append("constant", 5).append("month", 0).append("variable", dbList));
			
		//fake regionalFactors collection
		DBCollection regionalAdjFactorsColl = db.getCollection("regionalAdjustmentFactors");
		regionalAdjFactorsColl.insert(new BasicDBObject("state", "TN").append("modelName", "Model_Name").append("modelId", "35").append("factor", "0.1"));
		
		scoringSingletonObj = constructor.newInstance();
		
	}
	
	//a positive case for modelId 35 (for topology call)
	// the member has non-expired variables VARIABLE4 AND VARIABLE10, which are associated with modelId 35 
	//so, their values ad exp dates are set by their corresponding strategy
	@Test
	public void calcRTSChangesTest() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsTesting";
		
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1).append("10",0.4));

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("10", 1.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())).append(
								"10",
								new BasicDBObject("v", expected2.getValue()).append("e",
										expected2.getExpirationDateAsString()).append("f",
										expected2.getEffectiveDateAsString())));
		
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y").append("st_cd", "TN"));
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
			
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		ChangedMemberScore expectedChangedMemberScore = new ChangedMemberScore();
		//both variables VARIABLE4 and VARIABLE10 has strategies with expiration date set to 2 days
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		expectedChangedMemberScore.setModelId("35");
		expectedChangedMemberScore.setScore(0.09937568022504835);
		expectedChangedMemberScore.setMinDate(simpleDateFormat.format(date));
		expectedChangedMemberScore.setMaxDate(simpleDateFormat.format(date));
		expectedChangedMemberScore.setEffDate(simpleDateFormat.format(new Date()));
		
		Map<String, Change> expectedAllChanges = new HashMap<String, Change>();
		Change change = new Change(1, date);
		Change change2 = new Change(1.1, date);
		expectedAllChanges.put("VARIABLE4", change);
		expectedAllChanges.put("VARIABLE10", change2);
		Object expectedVar4Value = expectedAllChanges.get("VARIABLE4").getValue();
		Object expectedVar10Value = expectedAllChanges.get("VARIABLE10").getValue();
		Date expectedVar4Date = expectedAllChanges.get("VARIABLE4").getExpirationDate();
		Date expectedVar10Date = expectedAllChanges.get("VARIABLE10").getExpirationDate();
		
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
	
		int compareVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) expectedVar4Value));
		int compareVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) expectedVar10Value));
		
		int compareVal3 = new Double(expectedChangedMemberScore.getScore()).compareTo(new Double(actualChangedMemberScore.getScore()));
	
		Assert.assertEquals(compareVal, 0);
		Assert.assertEquals(compareVal2, 0);
		Assert.assertEquals(actualVar4Date, expectedVar4Date);
		Assert.assertEquals(expectedVar10Date, actualVar10Date);
		
		Assert.assertEquals(compareVal3, 0);
		Assert.assertEquals(expectedChangedMemberScore.getMinDate(), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(expectedChangedMemberScore.getMaxDate(), actualChangedMemberScore.getMaxDate());
	}
	
	//a positive case for modelId 35 (for topology call)
	//the incoming feed corresponds to a BOOST variable which boosts the score
	@Test
	public void calcRTSChangesTest2() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsTesting2";
		
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1).append("10",0.4));

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("10", 1.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())).append(
								"10",
								new BasicDBObject("v", expected2.getValue()).append("e",
										expected2.getExpirationDateAsString()).append("f",
										expected2.getEffectiveDateAsString())));
		
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y").append("st_cd", "TN"));
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		newChangesVarValueMap.put("BOOST_SYW_VARIABLE7", "0.1");
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		ChangedMemberScore expectedChangedMemberScore = new ChangedMemberScore();
		//both variables VARIABLE4 and VARIABLE10 has strategies with expiration date set to 2 days
		Date minDate = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		Date maxDate = new LocalDate(new Date()).plusDays(7).toDateMidnight().toDate();
		expectedChangedMemberScore.setModelId("35");
		expectedChangedMemberScore.setScore(0.10037568022504835);
		expectedChangedMemberScore.setMinDate(simpleDateFormat.format(minDate));
		expectedChangedMemberScore.setMaxDate(simpleDateFormat.format(maxDate));
		expectedChangedMemberScore.setEffDate(simpleDateFormat.format(new Date()));
		
		Map<String, Change> expectedAllChanges = new HashMap<String, Change>();
		//VARIABLE4 and VARIABLE10 are set with minDate as exp dates as their strategy is to expire these variables in two days
		Change change = new Change(1, minDate);
		Change change2 = new Change(1.1, minDate);
		//BOOST_SYW_VARIABLE7 is set with maxDate as exp dates as their strategy is to expire these variables in 7 days 
		Change change3 = new Change(0.1, maxDate);
		expectedAllChanges.put("VARIABLE4", change);
		expectedAllChanges.put("VARIABLE10", change2);
		expectedAllChanges.put("BOOST_SYW_VARIABLE7", change3);
		Object expectedVar4Value = expectedAllChanges.get("VARIABLE4").getValue();
		Object expectedVar10Value = expectedAllChanges.get("VARIABLE10").getValue();
		Object expectedBoostSywVar7Value = expectedAllChanges.get("BOOST_SYW_VARIABLE7").getValue();
		Date expectedVar4Date = expectedAllChanges.get("VARIABLE4").getExpirationDate();
		Date expectedVar10Date = expectedAllChanges.get("VARIABLE10").getExpirationDate();
		Date expectedBoostSywVar7Date = expectedAllChanges.get("BOOST_SYW_VARIABLE7").getExpirationDate();
		
		List<ChangedMemberScore> changedMemberScoresList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore actualChangedMemberScore = changedMemberScoresList.get(0);
		
		Map<String, Change> actualAllChanges =  memberRTSChanges.getAllChangesMap();
		Object actualVar4Value = actualAllChanges.get("VARIABLE4").getValue();
		Object actualVar10Value = actualAllChanges.get("VARIABLE10").getValue();
		Object actualBoostSywVar7Value = actualAllChanges.get("BOOST_SYW_VARIABLE7").getValue();
		Date actualVar4Date = actualAllChanges.get("VARIABLE4").getExpirationDate();
		Date actualVar10Date = actualAllChanges.get("VARIABLE10").getExpirationDate();
		Date actualBoostSywVar7Date = actualAllChanges.get("BOOST_SYW_VARIABLE7").getExpirationDate();
	
		int compareVarVal = new Integer((Integer) actualVar4Value).compareTo(new Integer((Integer) expectedVar4Value));
		int compareVarVal2 = new Double((Double) actualVar10Value).compareTo(new Double((Double) expectedVar10Value));
		int compareVarVal3 = new Double(Double.valueOf( actualBoostSywVar7Value.toString())).compareTo(new Double(Double.valueOf( expectedBoostSywVar7Value.toString())));
		
		int compareScoreVal3 = new Double(expectedChangedMemberScore.getScore()).compareTo(new Double(actualChangedMemberScore.getScore()));
	
		Assert.assertEquals(compareVarVal, 0);
		Assert.assertEquals(compareVarVal2, 0);
		Assert.assertEquals(compareVarVal3, 0);
		Assert.assertEquals(actualVar4Date, expectedVar4Date);
		Assert.assertEquals(expectedVar10Date, actualVar10Date);
		Assert.assertEquals(expectedBoostSywVar7Date, actualBoostSywVar7Date);
		
		Assert.assertEquals(compareScoreVal3, 0);
		Assert.assertEquals(expectedChangedMemberScore.getMinDate(), actualChangedMemberScore.getMinDate());
		Assert.assertEquals(expectedChangedMemberScore.getMaxDate(), actualChangedMemberScore.getMaxDate());
	}
		
	
	@AfterClass
	public static void cleanUp(){
		if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());
		SystemPropertyUtility.dropDatabase();
	}
}
