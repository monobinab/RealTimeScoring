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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import analytics.exception.RealTimeScoringException;
import analytics.util.objects.Boost;
import analytics.util.objects.BoosterModel;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ScoringSingletonTest {
	private static ScoringSingleton scoringSingletonObj;
	private static DB db;
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
		
		//this utility sets the system property and get the Db
		SystemPropertyUtility.setSystemProperty();
		db = SystemPropertyUtility.getDb();
		// We do not need instance of scoring singleton created by previous
		// tests. If different methods need different instances, move this to
		// @Before rather than before class
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		scoringSingletonObj = constructor.newInstance();
		
		/*DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));*/
		
		}

	// This test is to check whether changedMemberVariablesMap is getting populated
	// (positive case)
	@Test
	public void testCreateChangedVariablesMap() throws ConfigurationException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
	
		DBCollection changedMemberVar = db
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
		changedMemberVar.remove(new BasicDBObject("l_id",l_id));
		varIdToNameMap.setAccessible(false);
	}

	//this test ensures that there will be no modelIds to get scored, when there are no new variables from the incoming feed
	@Test
	public void getModelIdListNullNewChangesVarValueMapTest1() {
		Map<String, String> newChangesVarValueMap = null;
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}

	//if none of the variables in newChangesVarValueMap is found in variableModelsMap, modelLists which needs to be scored will be empty
	@Test
	public void getModelIdListNullNewChangesVarValueMapTest2() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, variableModelsMapContents);
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
		variableModelsMap.setAccessible(false);
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
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(51);
		modelLists2.add(30);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents
				.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists2);
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
		Set<Integer> actualModelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		// Expected modelIds
		Set<Integer> expectedModelLists = new HashSet<Integer>();
		expectedModelLists.add(48);
		expectedModelLists.add(35);
		expectedModelLists.add(51);
		expectedModelLists.add(30);
		Assert.assertEquals(expectedModelLists, actualModelList);
		variableModelsMap.setAccessible(false);

	}

	// if variableModelsMap does not contain any
	// of the variables from newChangesVarValueMap --
	// i.e. here variableModelsMap does not contain S_DSL_APP_INT_ACC_FTWR_TRS2
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
		Set<Integer> actualModelLists = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		// Expected modelIds
		Set<Integer> expectedModelLists = new HashSet<Integer>();
		expectedModelLists.add(48);
		expectedModelLists.add(35);
		Assert.assertEquals(expectedModelLists, actualModelLists);
		variableModelsMap.setAccessible(false);
	}

	// This test is to check whether createMemberVariableValueMap() returns null if
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

	// This test is to check the memberVariablesMap creation (positive case)
	@Test
	public void createMemberVariableValueMapPositiveCaseTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		String lId = "SearsTesting";
		//fake memberVariables collection
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId)
				.append("2269", 1).append("2270", 0.10455));
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
				"S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		modelsMapContent.put(48, monthModelMap2);
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);
		modelIdsList.add(48);
		
		Map<String, Object> expectedMemberVariables = scoringSingletonObj
				.createMemberVariableValueMap(lId, modelIdsList);
		Set<String> actualMemVarValue = new HashSet<String>();
		actualMemVarValue.add("2270");
		actualMemVarValue.add("2269");

		Assert.assertEquals(actualMemVarValue, expectedMemberVariables.keySet());
		
		memberVariables.remove(new BasicDBObject("l_id", lId));
		modelsMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
	}
	
		//if variables assocaited with a modelId, which needs to be scored is not there in memberVar collection,
		//it wont be populated in the memberVariablrs map
		@Test
		public void createMemberVariableValueMapPositiveCaseTest2()
				throws ConfigurationException, SecurityException,
				NoSuchFieldException, IllegalArgumentException,
				IllegalAccessException, RealTimeScoringException {

			String lId = "SearsTesting2";
			//fake memberVariables collection
			DBCollection memberVariables = db.getCollection("memberVariables");
			memberVariables.insert(new BasicDBObject("l_id", lId)
					.append("2269", 1).append("2270", 0.10455));
			
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
					0.002));
			variablesMap.put("S_DSL_APP_INT_ACC2", new Variable(
					"S_DSL_APP_INT_ACC2", 0.0915));
			Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
			variablesMap2.put("S_DSL_APP_INT_ACC3", new Variable("S_DSL_APP_INT_ACC3",
					0.002));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
			Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
			monthModelMap2.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap2));
			Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent.put(35, monthModelMap);
			modelsMapContent.put(48, monthModelMap2);
			Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
			variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
			variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
			variableNameToVidMapContents.put("S_DSL_APP_INT_ACC3", "2271");

			
			Set<Integer> modelIdsList = new HashSet<Integer>();
			modelIdsList.add(35);
			modelIdsList.add(48);
			Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj, modelsMapContent);

			Field variableNameToVidMap = ScoringSingleton.class
					.getDeclaredField("variableNameToVidMap");
			variableNameToVidMap.setAccessible(true);
			variableNameToVidMap.set(scoringSingletonObj,
					variableNameToVidMapContents);
			Map<String, Object> expectedMemberVariables = scoringSingletonObj
					.createMemberVariableValueMap(lId, modelIdsList);
			Set<String> actualMemVarValue = new HashSet<String>();
			actualMemVarValue.add("2270");
			actualMemVarValue.add("2269");

			Assert.assertEquals(actualMemVarValue, expectedMemberVariables.keySet());
			
			memberVariables.remove(new BasicDBObject("l_id", lId));
			modelsMap.setAccessible(false);
			variableNameToVidMap.setAccessible(false);
		}

	// This test is to check if variableNameToVidMap does not contain any of the
	// variables which are there in modelsMap
	// Acutal reason is variables collection does not contain a variable which is there in modelVariables collection
	// As variableFilter has null in it, it WAS THROWING NULL POINTER EXCEPTION.
	// Code is refactored and caught the custom exception with NULL VID information
	// Pls note: This was the reason for getting variableId as NULL, like null=1
	// (faced with Eddie's lid)
	/*@Test
	public void createVariableValueMapForNullVIDTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		
		String lId = "SearsTesting3";
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId)
				.append("2269", 1).append("2268", 0.10455));

		//variablesMap, modelsMap are populated from modelVaraibles collection
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

		//variableNameToVidMap is populated from variables collection
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);

		RealTimeScoringException realTimeScoreExc = null;
		try {
			scoringSingletonObj.createMemberVariableValueMap(lId,
					modelIdsList);
		} catch (RealTimeScoringException e) {
			realTimeScoreExc = e;
		}

		Assert.assertNotNull(realTimeScoreExc);
		memberVariables.remove(new BasicDBObject("l_id", lId));
		modelsMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
	}*/

	//testing the method with nulls
	@Test
	public void getBoostScoreNullCheckTest() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		double boost = scoringSingletonObj.getBoostScore(null, null);
		int compare = (new Double(0.0)).compareTo(new Double(boost));
		Assert.assertEquals(compare, 0);
	}

	// This test case is for testing the boost returned for non month modelId
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

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChanges.put("BOOST_S_HOME_6M_IND", change2);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
		variablesMap.put("BOOST_S_HOME_6M_IND", new Boost(
				"BOOST_S_HOME_6M_IND", 0.02, 0.01));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35);
		int comapreVal = new Double(0.138).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}

	// If the non month model and does not contain the incoming boost variables
	@Test
	public void getBoostScoreBoostVarNotPresentInModelsMapWithMonth0Test()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND",
				0.0015));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(27, "Model_Name2", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27);
		Assert.assertEquals(new Double(0.0), new Double(boost));
		modelsMap.setAccessible(false);
	}
	
	// This tests the boosts returned for month model
	@Test
	public void getBoostScoreBoostVarPresentInModelsMapWithCurrentMonthTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 2));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));

		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27);
		int comapreVal = new Double(2.024).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}


	// this tests the boost returned for a month model and it does not contain the incoming boost variable
	// ideally this should not happen
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

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChanges.put("BOOST_S_HOME_6M_IND", change2);

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
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}

	//this test returns boost value of 0.0 as the incoming variable is blackout variable
	@Test
	public void getBoostScoreBlackoutSetOn()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BLACKOUT_HA_COOK", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BLACKOUT_BROWSE_HA_COOK", new Boost(
				"BOOST_BROWSE_HA_COOK", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		
		modelsMap.setAccessible(false);
	}

	@Test
	public void getBoostScoreBlackoutANDBoostIncomingVar()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change3 = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChanges.put("BLACKOUT_HA_COOK", change3);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
		variablesMap.put("BLACKOUT_HA_COOK", new Variable(
				"BLACKOUT_HA_COOK", 1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}
	
	
	@Test
	public void getBoostScoreAllChangesNullTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		HashMap<String, Change> allChanges = null;
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
		variablesMap.put("BLACKOUT_HA_COOK", new Variable(
				"BLACKOUT_HA_COOK", 1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}
	
	@Test
	public void getBoostScoreModelIdNullTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost(
				"BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContentBoost);
		double boost = scoringSingletonObj.getBoostScore(allChanges, null);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}
	
	@Test
	public void getBoostScoreModelIdNotInModelsMapTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND",
				0.0015));
		Map<Integer, Model> monthModelMap= new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name4", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);

		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}

	//calc newScore positive case test
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
		Change change = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);

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
		int comapreVal = new Double(0.9935028049029226).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
	}

	// If memberVariables is null, expected to throw RealTimeScoringException
	// Ideally this should not happen
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
		Field varNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMap.setAccessible(true);
		varNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.calcScore(null, allChanges, 35);
		modelsMap.setAccessible(false);
		varNameToVidMap.setAccessible(false);
	}

	// If memberVariables as well as changedMemberVariables is null, expected to throw RealTimeScoringException
	// ideally this should not happen
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
		Field varNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMap.setAccessible(true);
		varNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		scoringSingletonObj.calcBaseScore(null, null, 35);
		modelsMap.setAccessible(false);
		varNameToVidMap.setAccessible(false);
		
	}

	// tests the baseScore for non-month model
	@Test
	public void calcBaseScorePositiveCaseWithMonth0Test() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		memVariables.put("2270", 0.10455);
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);

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
		Field varNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMap.setAccessible(true);
		varNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables,
				allChanges, 35);
		int comapreVal = new Double(5.0298663249999995).compareTo(new Double(baseScore));
		Assert.assertEquals(comapreVal, 0);
		modelsMap.setAccessible(false);
		varNameToVidMap.setAccessible(false);
	}

	// tests the baseScore for current month model
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
		Field varNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		varNameToVidMap.setAccessible(true);
		varNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables,
				allChanges, 35);
		int comapreVal = new Double(3.029866325).compareTo(new Double(baseScore));
		Assert.assertEquals(0, comapreVal);
		modelsMap.setAccessible(false);
		varNameToVidMap.setAccessible(false);
	}

	/*********
	 * strategy test cases
	 * @throws SecurityException
	 * @throws NoSuchFieldException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws ParseException
	 * @throws ConfigurationException
	 */
	@Test
	public void strategyCountTransactionsTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS",
				"StrategyCountTransactions");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", "2273");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2273", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap, memVariables );
		Assert.assertEquals(4, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS").getExpirationDateAsString());
		variableNameToStrategyMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		varaibleModelsMap.setAccessible(false);
	}

	
	@Test
	public void strategyDaysSinceLastTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
	
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", modelLists);
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2",
				"StrategyDaysSinceLast");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "2284");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2284", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2284", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS2", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap, memVariables );
		Assert.assertEquals(1, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getExpirationDateAsString());
		variableNameToStrategyMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		varaibleModelsMap.setAccessible(false);
			
	}
	
	@Test
	public void strategyTurnOnFlagTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", modelLists);
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2",
				"StrategyTurnOnFlag");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "2283");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2283", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2283", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS2", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap, memVariables );
		Assert.assertEquals(1, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getExpirationDateAsString());
		variableNameToStrategyMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		varaibleModelsMap.setAccessible(false);
			
	}

	@Test
	public void strategyTurnOffFlagTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", modelLists);
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2",
				"StrategyTurnOffFlag");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "2283");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2283", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2283", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS2", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap, memVariables );
		Assert.assertEquals(0, allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getExpirationDateAsString());
		variableNameToStrategyMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		varaibleModelsMap.setAccessible(false);
			
	}


	@Test
	public void strategyPurchaseOccasionsTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		
		DBCollection varColl = db.getCollection("Variables");
		varColl.insert(new BasicDBObject("name", "v1").append("VID", 1).append("strategy","StrategyCountTransactions"));
		varColl.insert(new BasicDBObject("name", "v2").append("VID", 2).append("strategy","StrategyCountTraitDates"));
		varColl.insert(new BasicDBObject("name", "v3").append("VID", 3).append("strategy","StrategyCountTraits"));
		varColl.insert(new BasicDBObject("name", "v4").append("VID", 4).append("strategy","StrategyDaysSinceLast"));
		varColl.insert(new BasicDBObject("name", "v5").append("VID", 5).append("strategy","StrategyTurnOnFlag"));
		varColl.insert(new BasicDBObject("name", "v6").append("VID", 6).append("strategy","StrategyTurnOffFlag"));
		varColl.insert(new BasicDBObject("name", "v7").append("VID", 7).append("strategy","StrategyBoostProductTotalCount"));
		varColl.insert(new BasicDBObject("name", "v8").append("VID", 8).append("strategy","StrategyDCFlag"));
		varColl.insert(new BasicDBObject("name", "v9").append("VID", 9).append("strategy","StrategyPurchaseOccasions"));
		varColl.insert(new BasicDBObject("name", "v10").append("VID", 10).append("strategy","StrategySumSales"));
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", modelLists);
		Field varaibleModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,variableModelsMapContents);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2",
				"StrategyPurchaseOccasions");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,variableNameToStrategyMapContents);
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "2283");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2283", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2283", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS2", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(
				allChanges, newChangesVarValueMap, memVariables );
		Assert.assertEquals("0.001", allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(365).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getExpirationDateAsString());
		variableNameToStrategyMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		varaibleModelsMap.setAccessible(false);
			
	}

	// This test case is checked with null modelIdScoreMap to update the changedMemberScore collection, 
	//i.e., if there is no re-scored value for the modelIdList, the original value and dates will be re-stored
	//In this test we can see that model id 51, changedMemScore value and dates are restored
	// This case will not happen at all, was just checking as an external class
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScoreNullModelIdScoreMapTest()
			throws SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException, ParseException,
			ConfigurationException, RealTimeScoringException {
		
		String lId = "SearsUpdate";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-09-12","test");
		changedMemberScore.insert(new BasicDBObject("l_id", lId)
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
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		
		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date minExp = dateFormat.parse("2999-05-19");
		Date maxExp = dateFormat.parse("2999-05-19");
		Map<Integer,Map<String,Date>> modelIdToExpiryMap = new HashMap<Integer, Map<String,Date>>();
		Map<String, Date> dateMap = new HashMap<String, Date>();
		dateMap.put("minExp", minExp);
		dateMap.put("maxExp", maxExp);
		modelIdToExpiryMap.put(51, dateMap);
		
		scoringSingletonObj.updateChangedMemberScore("SearsUpdate", modelIds,
				modelIdToExpiryMap, null, "test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				"SearsUpdate"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");

		Assert.assertEquals(0.02, changedMemScores51.get("s"));
		Assert.assertEquals(changedMemScore.getMinDate(),
				changedMemScores51.get("minEx"));
		modelsMap.setAccessible(false);
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}

	// this test is to check a positive case for updating changedMemberScore
	//For modelId 51, dates and scores are updated, for modelId 46, dates and scores are re-stored as modelIdScoreMap contains only modelId 51
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScorePositiveCaseTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException, RealTimeScoringException {
		String lId = "SearsUpdate2";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
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

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("SYW_WANT_TOYS_TCOUNT", new Variable(
				"SYW_WANT_TOYS_TCOUNT", 0.0015));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(51, "Model_Name5", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(51, monthModelMap);
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date minExp = simpleDateFormat.parse("2999-05-19");
		Date maxExp = simpleDateFormat.parse("2999-05-19");
		Map<Integer,Map<String,Date>> modelIdToExpiryMap = new HashMap<Integer, Map<String,Date>>();
		Map<String, Date> dateMap = new HashMap<String, Date>();
		dateMap.put("minExpiry", minExp);
		dateMap.put("maxExpiry", maxExp);
		modelIdToExpiryMap.put(51, dateMap);
	
		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		DBObject dbObj2 = changedMemberScore.findOne(new BasicDBObject("l_id",
				lId));
		System.out.println("changedMemScore before update " + dbObj2);
		scoringSingletonObj.updateChangedMemberScore(lId, modelIds,
				modelIdToExpiryMap, modelIdScoreMap,"test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				lId));
		System.out.println("changedMemScore after update " + dbObj);
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		HashMap<String, ChangedMemberScore> changedMemScores46 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("46");

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
		Date lastDayOfMonth = calendar.getTime();
		Assert.assertEquals(0.09, changedMemScores51.get("s"));
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth),
				changedMemScores51.get("minEx"));
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth),
				changedMemScores51.get("maxEx"));
		Assert.assertEquals(0.102, changedMemScores46.get("s"));
		Assert.assertEquals("2014-08-10",
				changedMemScores46.get("minEx"));
		
		modelsMap.setAccessible(false);
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	//testing insert and update
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScorePositiveCaseTest2() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException, RealTimeScoringException {
		String lId = "SearsUpdate2";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-10-04","test");
	/*	ChangedMemberScore changedMemScore2 = new ChangedMemberScore(0.102,
				"2014-08-10", "2014-08-20", "2014-10-04","test");*/
		changedMemberScore.insert(new BasicDBObject("l_id", "SearsUpdate2")
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
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(51, monthModelMap);
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date minExp = simpleDateFormat.parse("2999-05-19");
		Date maxExp = simpleDateFormat.parse("2999-05-19");
		Map<Integer,Map<String,Date>> modelIdToExpiryMap = new HashMap<Integer, Map<String,Date>>();
		Map<String, Date> dateMap = new HashMap<String, Date>();
		dateMap.put("minExpiry", minExp);
		dateMap.put("maxExpiry", maxExp);
		modelIdToExpiryMap.put(51, dateMap);
		Date minExp2 = simpleDateFormat.parse("2777-05-19");
		Date maxExp2 = simpleDateFormat.parse("2777-05-19");
		Map<String, Date> dateMap2 = new HashMap<String, Date>();
		dateMap2.put("minExpiry", minExp2);
		dateMap2.put("maxExpiry", maxExp2);
		modelIdToExpiryMap.put(46, dateMap2);
	
		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		modelIds.add(46);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		modelIdScoreMap.put(46, 0.102);
		DBObject dbObj2 = changedMemberScore.findOne(new BasicDBObject("l_id",
				lId));
		System.out.println("changedMemScore before update " + dbObj2);
		scoringSingletonObj.updateChangedMemberScore(lId, modelIds,
				modelIdToExpiryMap, modelIdScoreMap,"test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				lId));
		System.out.println("changedMemScore after update " + dbObj);
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		HashMap<String, ChangedMemberScore> changedMemScores46 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("46");

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
		Date lastDayOfMonth = calendar.getTime();
		Assert.assertEquals(0.09, changedMemScores51.get("s"));
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth),
				changedMemScores51.get("minEx"));
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth),
				changedMemScores51.get("maxEx"));
		Assert.assertEquals(0.102, changedMemScores46.get("s"));
		Assert.assertEquals("2777-05-19",
				changedMemScores46.get("minEx"));
		
		modelsMap.setAccessible(false);
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScoreNullMinMaxDatesTest() throws SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException, ConfigurationException, RealTimeScoringException {

		String lId = "SearsUpdate4";
		
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02,
				"2014-09-10", "2014-09-20", "2014-10-04", "test");
		
		changedMemberScore.insert(new BasicDBObject("l_id", lId)
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
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, modelsMapContent);

		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(51);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		
		Map<String, Date> dateMap = new HashMap<String, Date>();
		dateMap.put("minExpiry", null);
		dateMap.put("maxExpiry", null);
		Map<Integer,Map<String,Date>> modelIdToExpiryMap = new HashMap<Integer, Map<String,Date>>();
		modelIdToExpiryMap.put(51, dateMap);
	
		scoringSingletonObj.updateChangedMemberScore(lId, modelIds,
				modelIdToExpiryMap, modelIdScoreMap,"test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id",
				lId));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String today = simpleDateFormat.format(new Date());
		//The Min and Max dates have been intentionally passed as null to updatechangedMemeberScore and the output
		//should have the min ,max and eff dates set to today if nulls are passed in the input.
	
		Assert.assertEquals(changedMemScores51.get("minEx"), today);
		Assert.assertEquals(changedMemScores51.get("maxEx"), today);
		
		modelsMap.setAccessible(false);
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	// This is to check the update if all allChanges is null
	// The original values and dates of the existing changedMemberVar will be restored in the collection
	// Ideally it will not happen, was just checking as an external class
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesForNullChangedMemberVariablesTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {
				
		String l_id = "SearsUpdate3";
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");//changedMemberVariables
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
	
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		scoringSingletonObj.updateChangedVariables(l_id, null);
		DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id",
				l_id));
		HashMap<String, Object> map = (HashMap<String, Object>) dbObj
				.get("222");
		Double score = (Double) map.get("v");
		Assert.assertEquals(expected.getExpirationDateAsString(), map.get("e"));
		Assert.assertEquals(expected.getValue(), score);
		
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
	}

	// for a positive case, testing upsert, i.e new id is getting inserted (VID 333) and already existing one is getting updated (VID 222)
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesPositiveCaseTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		String l_id = "Example";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
	
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		Change change = new Change("222", 10.0,
				simpleDateFormat.parse("2888-11-20"),
				simpleDateFormat.parse("2014-10-04"));
		Change change2 = new Change("333", 1.7,
				simpleDateFormat.parse("2999-11-20"),
				simpleDateFormat.parse("2014-10-04"));
		HashMap<String, Change> allchanges = new HashMap<String, Change>();
		allchanges.put("MY_VAR_NAME", change);
		allchanges.put("MY_VAR_NAME2", change2);
		
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("MY_VAR_NAME", "222");
		variableNameToVidMapContents.put("MY_VAR_NAME2", "333");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,
				variableNameToVidMapContents);
		
		DBObject dbObject2 = changedMemberVar.findOne(new BasicDBObject("l_id",
				l_id));
			
		System.out.println("changedMemberVar before update" + dbObject2);

		
		scoringSingletonObj.updateChangedVariables(l_id,
				allchanges);

		DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id",
				l_id));
			
		System.out.println("changedMemberVar after update" + dbObject);

		// Testing update
		HashMap<String, Object> var222Map = (HashMap<String, Object>) dbObject.get("222");
		Assert.assertEquals(allchanges.get("MY_VAR_NAME").getValue(),
				var222Map.get("v"));
		Assert.assertEquals(allchanges.get("MY_VAR_NAME").getExpirationDateAsString(),
				var222Map.get("e"));
		Assert.assertEquals(allchanges.get("MY_VAR_NAME").getEffectiveDateAsString(),
				var222Map.get("f"));

		HashMap<String, Object> var333Map = (HashMap<String, Object>) dbObject.get("333");
		Assert.assertEquals(change2.getEffectiveDateAsString(),
				var333Map.get("f"));
		Assert.assertEquals(change2.getValue(),
				var333Map.get("v"));
		Assert.assertEquals(change2.getExpirationDateAsString(),
				var333Map.get("e"));
	
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		variableNameToVidMap.setAccessible(false);
	}
	@Test
	public void calcRegionalFactorPositiveCaseTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{

		//preparing regionalFactorsMap
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"TN", 0.2);
		Field regionalFactorsMap = ScoringSingleton.class
				.getDeclaredField("regionalFactorsMap");
		regionalFactorsMap.setAccessible(true);
		regionalFactorsMap.set(scoringSingletonObj, regionalFactorsMapContents);
		Double factor = scoringSingletonObj.calcRegionalFactor(35, "TN");
		Assert.assertEquals(0.2, factor);
		regionalFactorsMap.setAccessible(false);
	}

	@Test
	public void calcRegionalFactorWithEmptyRegionalFactorTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{

		//preparing regionalFactorsMap
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		Field regionalFactorsMap = ScoringSingleton.class
				.getDeclaredField("regionalFactorsMap");
		regionalFactorsMap.setAccessible(true);
		regionalFactorsMap.set(scoringSingletonObj, regionalFactorsMapContents);
		Double factor = scoringSingletonObj.calcRegionalFactor( 35, "TN");
		Assert.assertEquals(1.0, factor);
		regionalFactorsMap.setAccessible(false);
	}

	@Test
	public void calcRegionalWithNoRequiredModelIdTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{

		//preparing regionalFactorsMap
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"TN", 0.2);
		Field regionalFactorsMap = ScoringSingleton.class
				.getDeclaredField("regionalFactorsMap");
		regionalFactorsMap.setAccessible(true);
		regionalFactorsMap.set(scoringSingletonObj, regionalFactorsMapContents);
		Double factor = scoringSingletonObj.calcRegionalFactor( 46, "TN");
		Assert.assertEquals(1.0, factor);
		regionalFactorsMap.setAccessible(false);
	}

	@Test
	public void calcRegionalWithNoRequiredStateTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{

		//preparing regionalFactorsMap
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"IL", 0.2);
		Field regionalFactorsMap = ScoringSingleton.class
				.getDeclaredField("regionalFactorsMap");
		regionalFactorsMap.setAccessible(true);
		regionalFactorsMap.set(scoringSingletonObj, regionalFactorsMapContents);
		Double factor = scoringSingletonObj.calcRegionalFactor( 35, "TN");
		Assert.assertEquals(1.0, factor);
		regionalFactorsMap.setAccessible(false);
	}

	@Test
	public void calcRegionalWithNoStateForMemberTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Double factor = scoringSingletonObj.calcRegionalFactor(35, null);
		Assert.assertEquals(1.0, factor);
	}
	
	@Test
	public void calcBoosterScorePositiveCaseTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Set<Integer> boosterModelIdContents = new HashSet<Integer>();
		boosterModelIdContents.add(35);
		Field boosterModelIds = ScoringSingleton.class
				.getDeclaredField("boosterModelIds");
		boosterModelIds.setAccessible(true);
		boosterModelIds.set(scoringSingletonObj, boosterModelIdContents);
		Map<Integer, BoosterModel> boosterModelVariablesMapContents = new HashMap<Integer, BoosterModel>();
		Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
		boosterVariablesMap.put("Booster_Var", 1.2);
		boosterVariablesMap.put("MSM_SCORE", 0.5);
		BoosterModel boosterModel = new BoosterModel();
		boosterModel.setModelId(35);;
		boosterModel.setModelName("Booster_Model");
		boosterModel.setConstant(5);
		boosterModel.setMonth(5);
		boosterModel.setBoosterVariablesMap(boosterVariablesMap);
		boosterModelVariablesMapContents.put(35, boosterModel);
		Field boosterModelVariablesMap = ScoringSingleton.class
				.getDeclaredField("boosterModelVariablesMap");
		boosterModelVariablesMap.setAccessible(true);
		boosterModelVariablesMap.set(scoringSingletonObj, boosterModelVariablesMapContents);
		Map<String, String> boosterVariableNameToVidMapContents = new HashMap<String, String>();
		boosterVariableNameToVidMapContents.put("Booster_Var", "100");
		Field boosterVariableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("boosterVariableNameToVidMap");
		boosterVariableNameToVidMap.setAccessible(true);
		boosterVariableNameToVidMap.set(scoringSingletonObj, boosterVariableNameToVidMapContents);
		Map<String, Object> mbrBoosterVarMap = new HashMap<String, Object>();
		mbrBoosterVarMap.put("100", 1);
		double boosterScore = scoringSingletonObj.calcBoosterScore(mbrBoosterVarMap, 35, 0.0064);//6.2
		Assert.assertEquals(0.975337084392176, boosterScore);

		boosterModelVariablesMap.setAccessible(false);
		boosterVariableNameToVidMap.setAccessible(false);
		boosterModelIds.setAccessible(false);
	}

	@Test
	public void calcBoosterScoreNoReqBoosterVarInMemberTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Set<Integer> boosterModelIdContents = new HashSet<Integer>();
		boosterModelIdContents.add(35);
		Field boosterModelIds = ScoringSingleton.class
				.getDeclaredField("boosterModelIds");
		boosterModelIds.setAccessible(true);
		boosterModelIds.set(scoringSingletonObj, boosterModelIdContents);
		Map<Integer, BoosterModel> boosterModelVariablesMapContents = new HashMap<Integer, BoosterModel>();
		Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
		boosterVariablesMap.put("Booster_Var", 1.2);
		boosterVariablesMap.put("MSM_SCORE", 0.5);
		BoosterModel boosterModel = new BoosterModel();
		boosterModel.setModelId(35);;
		boosterModel.setModelName("Booster_Model");
		boosterModel.setConstant(5);
		boosterModel.setMonth(5);
		boosterModel.setBoosterVariablesMap(boosterVariablesMap);
		boosterModelVariablesMapContents.put(35, boosterModel);
		Field boosterModelVariablesMap = ScoringSingleton.class
				.getDeclaredField("boosterModelVariablesMap");
		boosterModelVariablesMap.setAccessible(true);
		boosterModelVariablesMap.set(scoringSingletonObj, boosterModelVariablesMapContents);
		Map<String, String> boosterVariableNameToVidMapContents = new HashMap<String, String>();
		boosterVariableNameToVidMapContents.put("Booster_Var", "100");
		Field boosterVariableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("boosterVariableNameToVidMap");
		boosterVariableNameToVidMap.setAccessible(true);
		boosterVariableNameToVidMap.set(scoringSingletonObj, boosterVariableNameToVidMapContents);
		Map<String, Object> mbrBoosterVarMap = new HashMap<String, Object>();
		mbrBoosterVarMap.put("101", 1);
		double boosterScore = scoringSingletonObj.calcBoosterScore(mbrBoosterVarMap, 35, 0.0064);//6.2
		Assert.assertEquals(0.0064, boosterScore);

		boosterModelVariablesMap.setAccessible(false);
		boosterVariableNameToVidMap.setAccessible(false);
		boosterModelIds.setAccessible(false);
	}

	@Test
	public void calcBoosterScoreReqModelNotInBoosterModeVarMapTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Set<Integer> boosterModelIdContents = new HashSet<Integer>();
		boosterModelIdContents.add(35);
		Field boosterModelIds = ScoringSingleton.class
				.getDeclaredField("boosterModelIds");
		boosterModelIds.setAccessible(true);
		boosterModelIds.set(scoringSingletonObj, boosterModelIdContents);
		Map<Integer, BoosterModel> boosterModelVariablesMapContents = new HashMap<Integer, BoosterModel>();
		Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
		boosterVariablesMap.put("Booster_Var", 1.2);
		boosterVariablesMap.put("MSM_SCORE", 0.5);
		BoosterModel boosterModel = new BoosterModel();
		boosterModel.setModelId(45);;
		boosterModel.setModelName("Booster_Model");
		boosterModel.setConstant(5);
		boosterModel.setMonth(5);
		boosterModel.setBoosterVariablesMap(boosterVariablesMap);
		boosterModelVariablesMapContents.put(45, boosterModel);
		Field boosterModelVariablesMap = ScoringSingleton.class
				.getDeclaredField("boosterModelVariablesMap");
		boosterModelVariablesMap.setAccessible(true);
		boosterModelVariablesMap.set(scoringSingletonObj, boosterModelVariablesMapContents);
		Map<String, String> boosterVariableNameToVidMapContents = new HashMap<String, String>();
		boosterVariableNameToVidMapContents.put("Booster_Var", "100");
		Field boosterVariableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("boosterVariableNameToVidMap");
		boosterVariableNameToVidMap.setAccessible(true);
		boosterVariableNameToVidMap.set(scoringSingletonObj, boosterVariableNameToVidMapContents);
		Map<String, Object> mbrBoosterVarMap = new HashMap<String, Object>();
		mbrBoosterVarMap.put("100", 1);
		double boosterScore = scoringSingletonObj.calcBoosterScore(mbrBoosterVarMap, 35, 0.0064);//6.2
		Assert.assertEquals(0.0064, boosterScore);

		boosterModelVariablesMap.setAccessible(false);
		boosterVariableNameToVidMap.setAccessible(false);
		boosterModelIds.setAccessible(false);
	}

	@Test
	public void calcBoosterScoreReqVarNotInBoosterVarNameToVIDMapTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Set<Integer> boosterModelIdContents = new HashSet<Integer>();
		boosterModelIdContents.add(35);
		Field boosterModelIds = ScoringSingleton.class
				.getDeclaredField("boosterModelIds");
		boosterModelIds.setAccessible(true);
		boosterModelIds.set(scoringSingletonObj, boosterModelIdContents);
		Map<Integer, BoosterModel> boosterModelVariablesMapContents = new HashMap<Integer, BoosterModel>();
		Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
		boosterVariablesMap.put("Booster_Var", 1.2);
		boosterVariablesMap.put("MSM_SCORE", 0.5);
		BoosterModel boosterModel = new BoosterModel();
		boosterModel.setModelId(35);;
		boosterModel.setModelName("Booster_Model");
		boosterModel.setConstant(5);
		boosterModel.setMonth(5);
		boosterModel.setBoosterVariablesMap(boosterVariablesMap);
		boosterModelVariablesMapContents.put(35, boosterModel);
		Field boosterModelVariablesMap = ScoringSingleton.class
				.getDeclaredField("boosterModelVariablesMap");
		boosterModelVariablesMap.setAccessible(true);
		boosterModelVariablesMap.set(scoringSingletonObj, boosterModelVariablesMapContents);
		Map<String, String> boosterVariableNameToVidMapContents = new HashMap<String, String>();
		boosterVariableNameToVidMapContents.put("Booster_Var2", "100");
		Field boosterVariableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("boosterVariableNameToVidMap");
		boosterVariableNameToVidMap.setAccessible(true);
		boosterVariableNameToVidMap.set(scoringSingletonObj, boosterVariableNameToVidMapContents);
		Map<String, Object> mbrBoosterVarMap = new HashMap<String, Object>();
		mbrBoosterVarMap.put("100", 1);
		double boosterScore = scoringSingletonObj.calcBoosterScore(mbrBoosterVarMap, 35, 0.0064);//6.2
		Assert.assertEquals(0.0064, boosterScore);

		boosterModelVariablesMap.setAccessible(false);
		boosterVariableNameToVidMap.setAccessible(false);
		boosterModelIds.setAccessible(false);
	}


	@Test
	public void calcBoosterScoreNullBoosterMemberVarEmptyTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Set<Integer> boosterModelIdContents = new HashSet<Integer>();
		boosterModelIdContents.add(35);
		Field boosterModelIds = ScoringSingleton.class
				.getDeclaredField("boosterModelIds");
		boosterModelIds.setAccessible(true);
		boosterModelIds.set(scoringSingletonObj, boosterModelIdContents);
		Map<Integer, BoosterModel> boosterModelVariablesMapContents = new HashMap<Integer, BoosterModel>();
		Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
		boosterVariablesMap.put("Booster_Var", 1.2);
		boosterVariablesMap.put("MSM_SCORE", 0.5);
		BoosterModel boosterModel = new BoosterModel();
		boosterModel.setModelId(35);;
		boosterModel.setModelName("Booster_Model");
		boosterModel.setConstant(5);
		boosterModel.setMonth(5);
		boosterModel.setBoosterVariablesMap(boosterVariablesMap);
		boosterModelVariablesMapContents.put(35, boosterModel);
		Field boosterModelVariablesMap = ScoringSingleton.class
				.getDeclaredField("boosterModelVariablesMap");
		boosterModelVariablesMap.setAccessible(true);
		boosterModelVariablesMap.set(scoringSingletonObj, boosterModelVariablesMapContents);
		Map<String, String> boosterVariableNameToVidMapContents = new HashMap<String, String>();
		boosterVariableNameToVidMapContents.put("Booster_Var", "100");
		Field boosterVariableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("boosterVariableNameToVidMap");
		boosterVariableNameToVidMap.setAccessible(true);
		boosterVariableNameToVidMap.set(scoringSingletonObj, boosterVariableNameToVidMapContents);
		Map<String, Object> mbrBoosterVarMap = new HashMap<String, Object>();
		double boosterScore = scoringSingletonObj.calcBoosterScore(mbrBoosterVarMap, 35, 0.0064);//6.2
		Assert.assertEquals(0.0064, boosterScore);

		boosterModelVariablesMap.setAccessible(false);
		boosterVariableNameToVidMap.setAccessible(false);
		boosterModelIds.setAccessible(false);
	}
	
	@Test
	public void filterScoringModelIdListTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
			Set<Integer> modelIdList = new HashSet<Integer>() {
			private static final long serialVersionUID = 1L;
			{
			  add(48);
			}};
			Set<Integer> expectedModelIdList = new HashSet<Integer>() {
				private static final long serialVersionUID = 1L;
				{
				  add(48);
				}};
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("VAR", new Variable(
					"VAR", 0.0015));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(
					Calendar.getInstance().get(Calendar.MONTH) + 1,
					new Model(48, "Model_Name", Calendar.getInstance().get(
							Calendar.MONTH) + 1, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent.put(48, monthModelMap);
			Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj, modelsMapContent);
			
			scoringSingletonObj.filterScoringModelIdList(modelIdList);
			Assert.assertEquals(expectedModelIdList, modelIdList);
			
			modelsMap.setAccessible(false);
	}
	
	
	@Test
	public void filterScoringModelIdListTest2() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
			Set<Integer> modelIdList = new HashSet<Integer>() {
			private static final long serialVersionUID = 1L;
			{
			  add(48);
			}};
			Set<Integer> expectedModelIdList = new HashSet<Integer>() {
				private static final long serialVersionUID = 1L;
				{
				  add(48);
				}};
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("VAR", new Variable(
					"VAR", 0.0015));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(
					0,
					new Model(48, "Model_Name", 0, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent.put(48, monthModelMap);
			Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj, modelsMapContent);
			
			scoringSingletonObj.filterScoringModelIdList(modelIdList);
			Assert.assertEquals(expectedModelIdList, modelIdList);
			
			modelsMap.setAccessible(false);
	}
	

	@Test
	public void filteScoringModelIdListTest3() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
			Set<Integer> modelIdList = new HashSet<Integer>() {
			private static final long serialVersionUID = 1L;
			{
			  add(48);
			}};
			Set<Integer> expectedModelIdList = new HashSet<Integer>() {
				private static final long serialVersionUID = 1L;
				{
				 
				}};
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("VAR", new Variable(
					"VAR", 0.0015));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(
					13,
					new Model(48, "Model_Name", 13, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent.put(48, monthModelMap);
			Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj, modelsMapContent);
			
			scoringSingletonObj.filterScoringModelIdList(modelIdList);
			Assert.assertEquals(expectedModelIdList, modelIdList);
			
			modelsMap.setAccessible(false);
	}
	
	@AfterClass
	public static void cleanUp(){
		SystemPropertyUtility.dropDatabase();
	}
}
