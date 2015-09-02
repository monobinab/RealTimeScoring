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
import com.mongodb.DBObject;

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
		varColl.insert(new BasicDBObject("name", "Blackout_variable").append("VID", 11).append("strategy","StrategyBlackout"));
		varColl.insert(new BasicDBObject("name", "variable12").append("VID", 12).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable40").append("VID", 40).append("strategy","NONE"));
		varColl.insert(new BasicDBObject("name", "variable13").append("VID", 13).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "variable14").append("VID", 14).append("strategy","StrategyDCStrengthSum"));
			
		//fake modelVariables collection
		DBCollection modeVarColl = db.getCollection("modelVariables");
		BasicDBList dbList = new BasicDBList();
		dbList.add(new BasicDBObject("name", "variable4").append("coefficient", 0.015));
		dbList.add(new BasicDBObject("name", "variable10").append("coefficient", 0.05));
		dbList.add(new BasicDBObject("name", "Boost_Syw_variable7").append("coefficient", 0.1).append("intercept", 0.0));
		modeVarColl.insert(new BasicDBObject("modelId", 35).append("modelName", "Model_Name").append("modelDescription", "Apparel").append("constant", 5).append("month", 0).append("variable", dbList));
		BasicDBList dbList2 = new BasicDBList();
		dbList2.add(new BasicDBObject("name", "Blackout_variable").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 46).append("modelName", "Model_Name2").append("modelDescription", "Tools").append("constant", 5).append("month", 0).append("variable", dbList2));
		BasicDBList dbList3 = new BasicDBList();
		dbList3.add(new BasicDBObject("name", "invalidVariable").append("coefficient", 0.015));
		modeVarColl.insert(new BasicDBObject("modelId", 48).append("modelName", "Model_Name3").append("modelDescription", "Home Appliances").append("constant", 5).append("month", 0).append("variable", dbList3));
		
		//fake regionalFactors collection
		DBCollection regionalAdjFactorsColl = db.getCollection("regionalAdjustmentFactors");
		regionalAdjFactorsColl.insert(new BasicDBObject("state", "TN").append("modelName", "Model_Name").append("modelId", "35").append("factor", "0.1"));
		scoringSingletonObj = constructor.newInstance();
	}
	

	private void getChangedMemberVarColl(String l_id)
			throws ParseException {
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
	}
	private void getMemberInfoColl(String l_id) {
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y").append("st_cd", "TN"));
	}
	private void getMemberVarCollection(String l_id) {
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("4", 1).append("10",0.4));
	}
	private Map<String, String> newChangesVarValueMap() {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.01");
		newChangesVarValueMap.put("VARIABLE10", "0.1");
		return newChangesVarValueMap;
	}

	private Map<String, String> getNewChangesBoostVarValueMap() {
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
		newChangesVarValueMap.put("BOOST_SYW_VARIABLE7", "0.1");
		return newChangesVarValueMap;
	}
	
	//a positive case for modelId 35 (for topology call)
	// the member has non-expired variables VARIABLE4 AND VARIABLE10, which are associated with modelId 35 
	//so, their values and exp dates are set by their corresponding strategy, and used in scoring
	@Test
	public void calcRTSChangesTest() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		
		String l_id = "SearsIntegrationTesting";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
	
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

	/*
	 * 	same test as calcRTSChangesTest except that the member has no state in memberInfo coll or does not have record in the collection
	 */
	@Test
	public void calcRTSChangesTestWithNoState() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting2";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
	
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		ChangedMemberScore expectedChangedMemberScore = new ChangedMemberScore();
		//both variables VARIABLE4 and VARIABLE10 has strategies with expiration date set to 2 days
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		expectedChangedMemberScore.setModelId("35");
		expectedChangedMemberScore.setScore(0.9937568022504835);
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
	
	/*
	 * a positive case for modelId 35 (for topology call)
		the incoming feed corresponds to a BOOST variable which boosts the score
		same data as calcRTSChanges with an additional Boost_syw_variable7 as the incoming var
		so, the score got boosted from 0.09937568022504835 (by calcRTSChanges test) to 0.10037568022504835
	 */
	@Test
	public void calcRTSChangesTestWithBoostScore() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting3";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = getNewChangesBoostVarValueMap();
				
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
	
	/*
	 * if the score gets boosted to be greater than 1, it is set to 1.0 again 
	 */
	@Test
	public void calcRTSChangesTestWithScoreBoostedGT1() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting3_2";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		Map<String, String> newChangesVarValueMap = getNewChangesBoostVarValueMap();
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		ChangedMemberScore expectedChangedMemberScore = new ChangedMemberScore();
		//both variables VARIABLE4 and VARIABLE10 has strategies with expiration date set to 2 days
		Date minDate = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		Date maxDate = new LocalDate(new Date()).plusDays(7).toDateMidnight().toDate();
		expectedChangedMemberScore.setModelId("35");
		expectedChangedMemberScore.setScore(1.0);
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
	
	/*
	 *if the member does not have variables of interest, i.e. memberVariablesMap is empty
	 *in this case, the member does not have variables 4 and 10 which are associated with model 35
	 *but these variables, because of previous rts scoring are non-expired in changedMemVariables and hence used for the current scoring 
	 */
	@Test
	public void calcRTSChangesTestEmptyMemVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting4";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("14", 1).append("20",0.4));
		getChangedMemberVarColl(l_id);
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
					
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
	
	/*
	 *  if a member does not have record in memberVariables collection i.e. memberVariablesMap is null,
	 *  then the member should not be scored
	 */
	@Test
	public void calcRTSChangesTestNullMemVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting5";
	
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		
		Assert.assertEquals("Expecting null memberRTSChanges, as memberVariablesMap is null", null, memberRTSChanges);
	}
	
	/*to test a black out model
	 *BLACKOUT_VARIABLE is associated with modelId 46 and is the new incoming variable for this member
	 *even before getting into the logic of scoring, it should be blacked out with a score of 0.0 
	 *with expiration dates set based on StrategyBlackout (30 days for now)*/
	@Test
	public void calcRTSChangesTestBlackoutVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting6";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);
		getMemberInfoColl(l_id);
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("BLACKOUT_VARIABLE", "0.01");
				
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		List<ChangedMemberScore> changedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		ChangedMemberScore changedMemberScore = changedMemberScoreList.get(0);
		
		Date expDate = new LocalDate(new Date()).plusDays(30).toDateMidnight().toDate();
		String expirationDate = simpleDateFormat.format(expDate);
		
		Assert.assertEquals("Expecting score of 0 as this model is blacked out", 0.0, changedMemberScore.getScore());
		Assert.assertEquals(expirationDate, changedMemberScore.getMinDate());
		Assert.assertEquals(expirationDate, changedMemberScore.getMaxDate());
	}
	
	/*
	 * to test if a variable (INVALIDVARIABLE)which is associated with a model 48 is there in our modelVariables collection )
	 * but not in variables collection at all or 
	 * does not have proper record for it in the variables collection, so there will be no proper name or VID for it
	 * then, the model (model 48 int his case) which gets affected by the variable will NOT be scored
	 */
	@Test
	public void calcRTSChangesTestInvalidVar() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting7";
		
		getMemberVarCollection(l_id);

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"4",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
						
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("INVALIDVARIABLE", "0.01");
		
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, newChangesVarValueMap, null, "TEST");
		Map<String, Change> actualAllChanges = memberRTSChanges.getAllChangesMap(); 
		List<ChangedMemberScore> actualChangedMemberScoreList = memberRTSChanges.getChangedMemberScoreList();
		
		//this member has one unexpired variable
		Assert.assertEquals("Expecting only unexpired variables in allChanges as the incoming var is a invalid variable", 1, actualAllChanges.keySet().size());
		Assert.assertEquals("Expecting an empty scorelist as the incoming invalidvariable for the model can not be used in scoring", new ArrayList<ChangedMemberScore>(), actualChangedMemberScoreList);
	}
	
	/*
	 * if a variable "variable4" is expired in changedMemberVariable, 
	 * its value should be picked up from MemberVariable collection, if exists
	 */
	@Test
	public void calcRTSChangesTestWithOneVarExp() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting8";
		
		getMemberVarCollection(l_id);

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2014-09-23"),
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
		
		getMemberInfoColl(l_id);
					
		Map<String, String> newChangesVarValueMap = newChangesVarValueMap();
	
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
	
	@Test
	public void calcRTSChangesWithEmptyNewChangesMapTest() throws SecurityException, NoSuchFieldException, ParseException, IllegalArgumentException, IllegalAccessException{
		String l_id = "SearsIntegrationTesting9";
		MemberRTSChanges memberRTSChanges = scoringSingletonObj.calcRTSChanges(l_id, new HashMap<String, String>(), null, "TEST");
		Assert.assertEquals(null, memberRTSChanges);;
	}
	
	/*
	 * api's call to scoring, a positive case
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void executeTest() throws ParseException{
		
		String l_id = "apiLid";
		getMemberVarCollection(l_id);
		getChangedMemberVarColl(l_id);

		//fake changedMemberScore collection
		//empty changedMemberScore collection before update
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
	
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		
		//this method updates the changedMemberScore collection
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScoresUpdated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("35");
		Assert.assertEquals(0.9946749823142578, changedMemScoresUpdated.get("s"));
		Assert.assertEquals("2999-09-23", changedMemScoresUpdated.get("minEx"));
		Assert.assertEquals("2999-09-23", changedMemScoresUpdated.get("maxEx"));
		Assert.assertEquals(0.9946749823142578, actuaModelIdStringScoreMap.get("35"));
		changedMemberScore.remove(new BasicDBObject("l_id", l_id));
		
	}
	
	@Test
	public void executeWithEmptyModelListsTest() throws ParseException{
		
		String l_id = "apiLid2";
		ArrayList<String> modelLists = new ArrayList<String>();
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		Assert.assertTrue("Expecting an emptymodelIdStringScoreMap as modelList passed is empty", actuaModelIdStringScoreMap.isEmpty());
	}
	
	@Test
	public void executeWithNullModelListsTest() throws ParseException{
		
		String l_id = "apiLid3";
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, null,  "TEST");
		Assert.assertTrue("Expecting an emptymodelIdStringScoreMap as modelList passed is empty", actuaModelIdStringScoreMap.isEmpty());
	}
	
	/**
	 *if allChanges is null, calcScore will throw exception, which gets caught in calcRTSChanges method
	 *so, changedMemberScoreList will be empty
	 ***/
	
	@Test
	public void executeWithNullAllChangesTest() throws ParseException{
		
		String l_id = "apiLid4";
		getMemberVarCollection(l_id);
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists, "TEST");
		Assert.assertEquals("Expecting an empty map as List of ChangedMemScore is empty returned by calcRTSChanges", new HashMap<String, Double>(), actuaModelIdStringScoreMap);
	}
	
	/*
	 * If allChanges (allChanges = changedMemVarMap as it is from api) does not contain any variable associated with modelId to be scored
	 * min max Expiry will be set with null
	 * so, changedMemberScore should be updated with current date for their expiration
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void executeWithAllChangesNotHavingVarsOfInterestForModelToBeScoredTest() throws ParseException{
		
		String l_id = "apiLid5";
		getMemberVarCollection(l_id);
		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("4", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"40",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
			
		//fake changedMemberScore collection
		//empty changedMemberScore collection before update
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
	
		ArrayList<String> modelLists = new ArrayList<String>();
		modelLists.add("35");
		HashMap<String, Double> actuaModelIdStringScoreMap = scoringSingletonObj.execute(l_id, modelLists,  "TEST");
		
		//this method updates the changedMemberScore collection
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", l_id));
		HashMap<String, ChangedMemberScore> changedMemScoresUpdated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("35");
		Assert.assertEquals(0.9935358588660986, changedMemScoresUpdated.get("s"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScoresUpdated.get("minEx"));
		Assert.assertEquals(simpleDateFormat.format(new Date()), changedMemScoresUpdated.get("maxEx"));
		Assert.assertEquals(0.9935358588660986, actuaModelIdStringScoreMap.get("35"));
		changedMemberScore.remove(new BasicDBObject("l_id", l_id));
		
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
