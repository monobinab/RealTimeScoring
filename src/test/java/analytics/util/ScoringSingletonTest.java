package analytics.util;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import analytics.exception.RealTimeScoringException;
import analytics.util.objects.Boost;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class ScoringSingletonTest {
	private static ScoringSingleton scoringSingletonObj;
	private static DB conn;
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException {
		System.setProperty("rtseprod", "test");
		// Below line ensures an empty DB rather than reusing a DB with values
		// in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		conn = DBConnection.getDBConnection();
		// We do not need instance of scoring singleton created by previous
		// tests. If different methods need different instances, move this to
		// @Before rather than before class
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		scoringSingletonObj = constructor.newInstance();

	}

	@Before
	public void setUp() throws Exception {

		
	}

	@After
	public void tearDown() throws Exception {
	}

	// This test is check whether changedMemberVaraiblesMap is getting populated
	// (positive case)
	@Test
	public void testCreateChangedVariablesMap() throws ConfigurationException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
	
		DBCollection changedMemberVar = conn
				.getCollection("changedMemberVariables");
		String l_id = "6RpGnW1XhFFBoJV+T9cT9ok=";

		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"2270",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		Map<String, String> varIdToNameMapContents = new HashMap<String, String>();
		varIdToNameMapContents.put("2270", "MY_VAR_NAME");
		Field varIdToNameMap = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		varIdToNameMap.setAccessible(true);
		varIdToNameMap.set(scoringSingletonObj, varIdToNameMapContents);

		Map<String, Change> changedVars = scoringSingletonObj
				.createChangedVariablesMap(l_id);
		Assert.assertTrue(changedVars.containsKey("MY_VAR_NAME"));
		Change actual = changedVars.get("MY_VAR_NAME");
		Assert.assertEquals(expected.getValue(), actual.getValue());
		Assert.assertEquals(expected.getEffectiveDateAsString(),
				actual.getEffectiveDateAsString());
		Assert.assertEquals(expected.getExpirationDate(),
				actual.getExpirationDate());
		Assert.assertEquals(expected.getChangeVariable(),
				actual.getChangeVariable());
	}

	@Test
	public void getModelIdListNullNewChangesVarValueMapTest1() {
		Map<String, String> newChangesVarValueMap = null;
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}

	@Test
	public void getModelIdListNullNullNewChangesVarValueMapTest2() {
		// no access to variableModelsMap, need to fake data, added a public
		// method, might not be best practice
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		// testTarget.setVariableModelsMap(null);
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}

	// This test is for a positive case, and return modelIdLists for
	// newChangesVarValueMap
	@Test
	public void getModelIdListPositiveCaseTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {

		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		modelLists.add(35);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents
				.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists3);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", modelLists);

		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, variableModelsMapContents);
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_ALL", "1");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_MEM", "1.0");

		// Actual modelIds from ScoringSingleton
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		// Expected modelIds
		Set<Integer> result = new HashSet<Integer>();
		result.add(48);
		result.add(35);
		result.add(51);
		result.add(30);
		Assert.assertEquals(result, modelList);

	}

	// This test case is for checking if variableModelsMap does not contain any
	// of the variables from newChangesVarValueMap
	// here variableModelsMap does not contain S_DSL_APP_INT_ACC_FTWR_TRS2
	// The method is skipping that variable perfectly while populating
	// modelIdLists which needs to be re-scored
	@Test
	public void getModelIdListForVaraibleNotPresentInVaraibleModelsMapTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		modelLists.add(48);

		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR", modelLists);

		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, variableModelsMapContents);
		// Actual modelIds from ScoringSingleton
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		// Expected modelIds
		Set<Integer> result = new HashSet<Integer>();
		result.add(48);
		result.add(35);
		Assert.assertEquals(result, modelList);
	}

	// This test is to check whether createVariableValueMap() returns null if
	// loyaltyid is null
	@Test
	public void createVariableValueMapForNullLoyaltyIdTest()
			throws RealTimeScoringException {

		Map<String, String> newChangesVarValueMap2 = new HashMap<String, String>();
		newChangesVarValueMap2.put("S_HOME_6M_IND2", "value");
		Set<Integer> modelIdList2 = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap2);
		Map<String, Object> map = scoringSingletonObj.createMemberVariableValueMap(
				"", modelIdList2);
		assertEquals(map, null);
	}

	// This test is to check whether memberVariablesMap is created (positive
	// case)
	@Test
	public void createVariableValueMapPositiveCaseTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
				"S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap4 = new HashMap<Integer, Model>();
		monthModelMap4.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		modelsMapContent.put(48, monthModelMap4);
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND", "2268");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");

		DB db = DBConnection.getDBConnection();
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", "SearsTesting")
				.append("2269", 1).append("2270", 0.10455));
		Set<Integer> modelIdsList3 = new HashSet<Integer>();
		modelIdsList3.add(35);
		modelIdsList3.add(48);
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);

		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		Map<String, Object> variableValueMap = scoringSingletonObj
				.createMemberVariableValueMap("SearsTesting", modelIdsList3);
		Set<String> memVarValue = new HashSet<String>();
		memVarValue.add("2270");
		memVarValue.add("2269");

		Assert.assertEquals(memVarValue, variableValueMap.keySet());
	}

	// This test is to check if variableNameToVidMap does not contain any of the
	// variables which are there in modelsMap
	// As variableFilter has null in it, it WAS THROWING NULL POINTER EXCEPTION.
	// Code is refactored and caught the custom exception with NULL VID
	// information
	// Pls note: This was the reason for getting variableId as NULL, like null=1
	// (faced with Eddie's lid)
	@Test
	public void createVariableValueMapForNullVIDTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		DB db = DBConnection.getDBConnection();
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", "SearsTestingCheck")
				.append("2269", 1).append("2268", 0.10455));

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap
				.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);

		Set<Integer> modelIdsList3 = new HashSet<Integer>();
		modelIdsList3.add(35);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");

		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		RealTimeScoringException realTimeScoreExc = null;
		try {
			scoringSingletonObj.createMemberVariableValueMap("SearsTestingCheck",
					modelIdsList3);
		} catch (RealTimeScoringException e) {
			realTimeScoreExc = e;
		}

		Assert.assertNotNull(realTimeScoreExc);
	}

	@Test
	public void getBoostScoreNullCheckTest() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		double boost = scoringSingletonObj.getBoostScore(null, null);
		Assert.assertEquals(0.0, boost);
	}

	// This test case is for general checking with boost var present in
	// modelsMap with month 0
	@Test
	public void getBoostScoreBoostVarPresentInModelsMapWithMonth0Test()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChangesBoost = new HashMap<String, Change>();
		allChangesBoost.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChangesBoost.put("BOOST_S_HOME_6M_IND", change2);

		Map<String, Variable> variablesMapBoost = new HashMap<String, Variable>();
		variablesMapBoost.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
		variablesMapBoost.put("BOOST_S_HOME_6M_IND", new Boost(
				"BOOST_S_HOME_6M_IND", 0.02, 0.01));
		Map<Integer, Model> monthModelMapBoost = new HashMap<Integer, Model>();

		monthModelMapBoost.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMapBoost));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMapBoost);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChangesBoost, 35);
		Assert.assertEquals(0.138, boost);
	}

	// If the modelsMap month is 0 but does not contain the boost variables
	@Test
	public void getBoostScoreBoostVarNotPresentInModelsMapWithMonth0Test()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChangesBoost = new HashMap<String, Change>();
		allChangesBoost.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChangesBoost.put("BOOST_S_HOME_6M_IND", change2);

		Map<String, Variable> variablesMapBoost2 = new HashMap<String, Variable>();
		variablesMapBoost2.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND",
				0.0015));
		Map<Integer, Model> monthModelMapBoost2 = new HashMap<Integer, Model>();
		monthModelMapBoost2.put(0, new Model(27, "Model_Name2", 0, 5,
				variablesMapBoost2));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost2 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost2.put(27, monthModelMapBoost2);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost2);
		double boost = scoringSingletonObj.getBoostScore(allChangesBoost, 27);
		Assert.assertEquals(0.0, boost);
	}

	@Test
	public void getBoostScoreBlackoutSetOn()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change changeBlk1 = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change changeBlk2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		Change changeBlk3 = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChangesBoostBlk = new HashMap<String, Change>();
		allChangesBoostBlk.put("BOOST_S_DSL_APP_INT_ACC", changeBlk1);
		allChangesBoostBlk.put("BOOST_BROWSE_HA_COOK", changeBlk2);
		allChangesBoostBlk.put("BLACKOUT_HA_COOK", changeBlk3);

		Map<String, Variable> variablesMapBoostBlk = new HashMap<String, Variable>();
		variablesMapBoostBlk.put("BOOST_BROWSE_HA_COOK", new Boost(
				"BOOST_BROWSE_HA_COOK", 0.002, 0.1));
		variablesMapBoostBlk.put("BOOST_S_HOME_6M_IND", new Variable(
				"BOOST_S_HOME_6M_IND", 1));
		Map<Integer, Model> monthModelMapBoostBlk = new HashMap<Integer, Model>();

		monthModelMapBoostBlk.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMapBoostBlk));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMapBoostBlk);

		Field modelsMapBlk = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMapBlk.setAccessible(true);
		modelsMapBlk.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChangesBoostBlk, 35);
		Assert.assertEquals(0.0, boost);
	}

	// If the modelsMap month is current month and if it contains the boost
	// variables
	@Test
	public void getBoostScoreBoostVarPresentInModelsMapWithCurrentMonthTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChangesBoost = new HashMap<String, Change>();
		allChangesBoost.put("BOOST_S_DSL_APP_INT_ACC", change);

		Map<String, Variable> variablesMapBoost3 = new HashMap<String, Variable>();
		variablesMapBoost3.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 2));
		Map<Integer, Model> monthModelMapBoost3 = new HashMap<Integer, Model>();
		monthModelMapBoost3.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name3", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMapBoost3));

		Map<Integer, Map<Integer, Model>> modelsMapContentBoost3 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost3.put(27, monthModelMapBoost3);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost3);
		double boost = scoringSingletonObj.getBoostScore(allChangesBoost, 27);
		Assert.assertEquals(2.024, boost);
	}

	// If the modelsMap month is current month and if it does not contain the
	// boost variables
	@Test
	public void getBoostScoreBoostVarNotPresentInModelsMapWithCurrentMonthTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChangesBoost = new HashMap<String, Change>();
		allChangesBoost.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChangesBoost.put("BOOST_S_HOME_6M_IND", change2);

		Map<String, Variable> variablesMapBoost4 = new HashMap<String, Variable>();
		variablesMapBoost4.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND",
				0.0015));
		Map<Integer, Model> monthModelMapBoost4 = new HashMap<Integer, Model>();
		monthModelMapBoost4.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name4", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMapBoost4));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost4 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost4.put(27, monthModelMapBoost4);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost4);
		double boost = scoringSingletonObj.getBoostScore(allChangesBoost, 27);
		Assert.assertEquals(0.0, boost);
	}

	// This is to test whether calcScore() method is returning the correct
	// newScore (positive case)
	// checked with manually calculated value
	@Test
	public void calcScorePositveCaseTest() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ConfigurationException,
			RealTimeScoringException {
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		memVariables.put("2270", 0.10455);
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change2);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
				"S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		Field varNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMapContents.setAccessible(true);
		varNameToVidMapContents.set(scoringSingletonObj,
				variableNameToVidMapContents);
		double newScore = scoringSingletonObj.calcScore(memVariables,
				allChanges, 35);
		Assert.assertEquals(0.9935028049029226, newScore);
	}

	// If memberVariables is null, expected to throw RealTimeScoringException
	@Test(expected = RealTimeScoringException.class)
	public void calcScoreForNullMemberVariablesTest() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ConfigurationException,
			RealTimeScoringException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change2);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		Field varNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMapContents.setAccessible(true);
		varNameToVidMapContents.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.calcScore(null, allChanges, 35);
	}

	// If memberVariables as well as changedMemberVariables is null, expected to
	// throw RealTimeScoringException
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForNullMemberVarAndChangedMemVarTest() throws Exception,
			RealTimeScoringException {

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		Field varNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMapContents.setAccessible(true);
		varNameToVidMapContents.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.calcBaseScore(null, null, 35);
	}

	// This test case is tested with variables in membervariables or in
	// varChanges, month is 0
	@Test
	public void calcBaseScorePositiveCaseWithMonth0Test() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		memVariables.put("2270", 0.10455);
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change2);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
				"S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		Field varNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMapContents.setAccessible(true);
		varNameToVidMapContents.set(scoringSingletonObj,
				variableNameToVidMapContents);
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables,
				allChanges, 35);
		Assert.assertEquals(5.029866325, baseScore,5);
	}

	// This test case is tested with variables in membervariables or in
	// varChanges, month is current
	@Test
	public void calcBaseScorePositiveCaseWithCurrentMonthTest() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		memVariables.put("2270", 0.10455);
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change2);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
				"S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 3, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		Field varNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMapContents.setAccessible(true);
		varNameToVidMapContents.set(scoringSingletonObj,
				variableNameToVidMapContents);
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables,
				allChanges, 27);
		Assert.assertEquals(3.029866325, baseScore,5);
	}

	// If changedMemberVariables is empty in case
	// changedMemberVariblesMap will be populated with newChangesVaribleValueMap
	// which needs re-scoring
	// in executeStrategy(), changedMemberVariables map gets updated with new
	// values, dates etc
	@Test
	public void executeStrategyWithEmptyChangedMemberVariablesTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException {
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategySumSales"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyTurnOffFlag"));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_ALL", "1");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_MEM", "1.0");
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		modelLists.add(35);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists3);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS",
				"StrategyCountTransactions");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL",
				"StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM",
				"StrategyTurnOffFlag");
				
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", "2273");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", "2274");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", "2275");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
				
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);

		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);

		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		Map<String, Change> emptyAllChangesMap = new HashMap<String, Change>();
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Map<String, Change> allChanges = scoringSingletonObj.executeStrategy(
				emptyAllChangesMap, newChangesVarValueMap, memVariables);
		Assert.assertEquals(3, allChanges.size());
		Assert.assertEquals(0, allChanges.get("S_DSL_APP_INT_ACC_FTWR_MEM")
				.getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), allChanges.get("S_DSL_APP_INT_ACC_FTWR_MEM")
				.getExpirationDateAsString());
	}

	// This is to check executeStrategy() method (positive case)
	//Note for variables S_DSL_APP_INT_ACC_FTWR_TRS and S_DSL_APP_INT_ACC_FTWR_MEM, exp date and value updated based on strategy
	//for variable S_DSL_APP_INT_ACC_FTWR_ALL, expiration date and value are from changedMemVariables
	@Test
	public void executeStrategyPositiveCaseTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException {
		DB db = DBConnection.getDBConnection();
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategySumSales"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyTurnOffFlag"));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_MEM", "1.0");
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		modelLists.add(35);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists3);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS",
				"StrategyCountTransactions");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL",
				"StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM",
				"StrategyTurnOffFlag");
				
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", "2273");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", "2274");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", "2275");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
		Change change4 = new Change("2273", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change5 = new Change("2274", 0.12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_ALL", change4);
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS", change5);
				
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);

		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);

		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap,memVariables );
		Assert.assertEquals(3, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_ALL")
				.getValue());
		Assert.assertEquals(1, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS")
				.getValue());
		Assert.assertEquals(0, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_MEM")
				.getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date())
				.plusDays(2).toDateMidnight().toDate()),
				allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS")
						.getExpirationDateAsString());
		Assert.assertEquals("2999-10-21",
				allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_ALL")
				.getExpirationDateAsString());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date())
		.plusDays(2).toDateMidnight().toDate()),
		allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_MEM")
				.getExpirationDateAsString());


	}

	// This test case is checked with null modelIdScoreMap, 
	//i.e., if there is no re-scored value for the modelIdList, the original value and dates will be re-stored
	//In this test we can see that model id 51, changedMemScore value and dates are restored
	// This case will not happen at all, was just checking as an external class
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScoreNullModelIdScoreMapTest()
			throws SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException, ParseException,
			ConfigurationException, RealTimeScoringException {
		DB conn = DBConnection.getDBConnection();
		
		DBCollection changedMemberScore = conn.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-09-12","test");
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore(0.102,
				"2014-08-10", "2014-08-20", "2014-08-12","test");
		changedMemberScore.insert(new BasicDBObject("l_id", "SearsUpdate")
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
				.append("46",
						new BasicDBObject("s", changedMemScore2.getScore())
								.append("minEx", changedMemScore2.getMinDate())
								.append("maxEx", changedMemScore2.getMaxDate())
								.append("f", changedMemScore2.getEffDate())));

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("SYW_WANT_TOYS_TCOUNT", new Variable(
				"SYW_WANT_TOYS_TCOUNT", 0.0015));

		Map<Integer, Model> monthModelMapBoost5 = new HashMap<Integer, Model>();
		monthModelMapBoost5.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(51, "Model_Name5", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));

		Map<Integer, Map<Integer, Model>> modelsMapContentBoost5 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost5.put(51, monthModelMapBoost5);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2270", 10,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChangesSyw = new HashMap<String, Change>();
		allChangesSyw.put("SYW_WANT_TOYS_TCOUNT", change);
		allChangesSyw.put("SYW_WANT_TOYS_TCOUNT2", change2);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(46);
		modelLists2.add(30);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT", modelLists2);
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT2",modelLists3);
		
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj,
				variableModelsMapContents);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost5);

		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);

		scoringSingletonObj.updateChangedMemberScore("SearsUpdate", modelIds,
				allChangesSyw, null, "test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				"SearsUpdate"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");

		Assert.assertEquals(0.02, changedMemScores51.get("s"));
		Assert.assertEquals(changedMemScore.getMinDate(),
				changedMemScores51.get("minEx"));
	}

	// this test is to check a positive case
	//For modelId 51, dates and scores are updated
	//For modelId 46, dates and scores are re-stored
	//As modelIdScoreMap contains only modelId 51
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScorePositiveCaseTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException, RealTimeScoringException {

		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberScore = conn.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-10-04","test");
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore(0.102,
				"2014-08-10", "2014-08-20", "2014-10-04","test");
		changedMemberScore.insert(new BasicDBObject("l_id", "SearsUpdate2")
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
				.append("46",
						new BasicDBObject("s", changedMemScore2.getScore())
								.append("minEx", changedMemScore2.getMinDate())
								.append("maxEx", changedMemScore2.getMaxDate())
								.append("f", changedMemScore2.getEffDate())));

		Map<String, Variable> variablesMapBoost5 = new HashMap<String, Variable>();
		variablesMapBoost5.put("SYW_WANT_TOYS_TCOUNT", new Variable(
				"SYW_WANT_TOYS_TCOUNT", 0.0015));

		Map<Integer, Model> monthModelMapBoost5 = new HashMap<Integer, Model>();
		monthModelMapBoost5.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(51, "Model_Name5", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMapBoost5));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost5 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost5.put(51, monthModelMapBoost5);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2270", 12.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 10.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChangesSywBoost = new HashMap<String, Change>();
		allChangesSywBoost.put("SYW_WANT_TOYS_TCOUNT", change);
		allChangesSywBoost.put("SYW_WANT_TOYS_TCOUNT2", change2);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(46);
		modelLists2.add(30);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT", modelLists2);
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT2",modelLists3);

		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj,
				variableModelsMapContents);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost5);

		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		scoringSingletonObj.updateChangedMemberScore("SearsUpdate2", modelIds,
				allChangesSywBoost, modelIdScoreMap,"test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				"SearsUpdate2"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		HashMap<String, ChangedMemberScore> changedMemScores46 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("46");

		Assert.assertEquals(0.09, changedMemScores51.get("s"));
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
		Date lastDayOfMonth = calendar.getTime();
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth),
				changedMemScores51.get("minEx"));
		Assert.assertEquals(0.102, changedMemScores46.get("s"));
		Assert.assertEquals("2014-08-10",
				changedMemScores46.get("minEx"));
	}

	
	@SuppressWarnings("unchecked")
	@Test(expected = RealTimeScoringException.class)
	@Ignore
	public void updateChangedMemberScoreNullMinMaxDatesTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException, RealTimeScoringException {

		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberScore = conn.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-10-04","null");
		
		changedMemberScore.insert(new BasicDBObject("l_id", "SearsUpdate3")
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
				);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("SYW_WANT_TOYS_TCOUNT", new Variable(
				"SYW_WANT_TOYS_TCOUNT", 0.0015));

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(51, "Model_Name5", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent= new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(51, monthModelMap);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2270", 12.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2271", 10.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("SYW_WANT_TOYS_TCOUNT", change);
		allChanges.put("SYW_WANT_TOYS_TCOUNT2", change2);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(46);
		modelLists2.add(30);
				
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT", modelLists2);
		variableModelsMapContents.put("SYW_WANT_TOYS_TCOUNT2",modelLists2);

		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj,
				variableModelsMapContents);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);

		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		scoringSingletonObj.updateChangedMemberScore("SearsUpdate3", modelIds,
				allChanges, modelIdScoreMap,"null");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				"SearsUpdate3"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		
	}

	
	
	// This is to check the update if all changedMemVariables is null
	// The original values and dates will be restored as no variables are re-scored
	// Ideally it will not happen, was just checking as an external class
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesForNullChangedMemberVariablesTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberVar = conn.getCollection("changedMemberVariables");
		String l_id = "SearsUpdate3";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		scoringSingletonObj.updateChangedVariables("SearsUpdate3", null);
		DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id",
				"SearsUpdate3"));
		HashMap<String, Object> map = (HashMap<String, Object>) dbObj
				.get("222");
		Double score = (Double) map.get("v");
		Assert.assertEquals(expected.getExpirationDateAsString(), map.get("e"));
		Assert.assertEquals(expected.getValue(), score);
	}

	// Ideally it will not happen, just checking as an external class
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesNullChangedMemVarAndNullModelIdCheckTest2()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberVar = conn.getCollection("changedMemberVariables");
		String l_id = "SearsUpdate4";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		scoringSingletonObj.updateChangedVariables("SearsUpdate4", null);
		DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id",
				"SearsUpdate4"));
		HashMap<String, Object> map = (HashMap<String, Object>) dbObj
				.get("222");
		Double score = (Double) map.get("v");
		Assert.assertEquals(expected.getExpirationDateAsString(), map.get("e"));
		Assert.assertEquals(expected.getValue(), score);
	}

	// to check if modelId is null. Seems like modelId DOES NOT HAVE ANY WFFECT
	// WHEN UPDATING CHANGEDMEMBER VARIABLES
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesNullModelIdCheckTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberVar = conn.getCollection("changedMemberVariables");
		String l_id = "SearsUpdate5";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		Change change = new Change("222", 10,
				simpleDateFormat.parse("2999-11-20"),
				simpleDateFormat.parse("2014-10-04"));
		HashMap<String, Change> allVarchanges = new HashMap<String, Change>();
		allVarchanges.put("MY_VAR_NAME", change);
		HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("MY_VAR_NAME", "222");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.updateChangedVariables("SearsUpdate5", 
				allVarchanges);
		DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id",
				"SearsUpdate5"));
		HashMap<String, Object> var222Map = (HashMap<String, Object>) dbObject
				.get("222");
		Assert.assertEquals(allVarchanges.get("MY_VAR_NAME")
				.getExpirationDateAsString(), var222Map.get("e"));
		Assert.assertEquals(allVarchanges.get("MY_VAR_NAME")
				.getValue(), var222Map.get("v"));
	}

	// for a positive case
	// tested upsert, i.e new id is getting inserted (VID 333) and already existing one is
	// getting updated (VID 222)
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesPositiveCaseTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberVar = conn.getCollection("changedMemberVariables");
		String l_id = "SearsUpdate6";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		// Testing update
		Change change = new Change("222", 10.0,
				simpleDateFormat.parse("2888-11-20"),
				simpleDateFormat.parse("2014-10-04"));
		Change change2 = new Change("333", 1.7,
				simpleDateFormat.parse("2999-11-20"),
				simpleDateFormat.parse("2014-10-04"));
		HashMap<String, Change> allVarchanges = new HashMap<String, Change>();
		allVarchanges.put("MY_VAR_NAME", change);
		allVarchanges.put("MY_VAR_NAME2", change2);
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("MY_VAR_NAME", "222");
		variableNameToVidMapContents.put("MY_VAR_NAME2", "333");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.updateChangedVariables("SearsUpdate6",
				allVarchanges);

		DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id",
				"SearsUpdate6"));
		DBObject dbObject2 = (DBObject) dbObject.get("222");
		dbObject.removeField("_id");
		dbObject.removeField("l_id");
		Assert.assertEquals(allVarchanges.size(), dbObject.keySet().size());
		Assert.assertEquals(allVarchanges.get("MY_VAR_NAME").getValue(),
				dbObject2.get("v"));
		Assert.assertEquals(allVarchanges.get("MY_VAR_NAME").getExpirationDateAsString(),
				dbObject2.get("e"));
		Assert.assertEquals(allVarchanges.get("MY_VAR_NAME").getEffectiveDateAsString(),
				dbObject2.get("f"));

		// Testing insert
		scoringSingletonObj.updateChangedVariables("Sears2", allVarchanges);
		DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id",
				"Sears2"));
		HashMap<String, Object> var333Map = (HashMap<String, Object>) dbObject.get("333");
		Assert.assertEquals(change2.getEffectiveDateAsString(),
				var333Map.get("f"));
		
		// Expected size is 4 as it includes _id and l_id also
		Assert.assertNotNull(dbObj);
		Assert.assertEquals(4, dbObj.keySet().size());
		changedMemberVar.remove(new BasicDBObject("l_id", "Sears2"));
	}
	@AfterClass
	public static void cleanUp(){
		conn.dropDatabase();
	}
	
}
