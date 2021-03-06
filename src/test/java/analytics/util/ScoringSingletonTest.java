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
import org.junit.Ignore;
import org.junit.Test;

import analytics.exception.RealTimeScoringException;
import analytics.util.objects.Boost;
import analytics.util.objects.Change;
import analytics.util.objects.ChangedMemberScore;
import analytics.util.objects.Model;
import analytics.util.objects.RegionalFactor;
import analytics.util.objects.Variable;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * @author kmuthuk
 *
 */
public class ScoringSingletonTest {
	private static ScoringSingleton scoringSingletonObj;
	private static DB db;
	private static FakeMongoStaticCollection fakeMongoStaticCollection;
	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException, SecurityException, NoSuchFieldException {
		
		//this utility sets the system property and get the Db
		SystemPropertyUtility.setSystemProperty();
		fakeMongoStaticCollection = new FakeMongoStaticCollection();
		db = SystemPropertyUtility.getDb();
		// We do not need instance of scoring singleton created by previous
		// tests. If different methods need different instances, move this to
		// @Before rather than before class
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		scoringSingletonObj = constructor.newInstance();
		
		//fake variables collection
		/*DBCollection varColl = db.getCollection("Variables");
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
		varColl.insert(new BasicDBObject("name", "variable14").append("VID", 14).append("strategy","StrategyDCStrengthSum"));*/
		
	}

	/*this test ensures that there will be no modelIds to get scored, when there are no new variables from the incoming feed
	i.e. newChangesVarValueMap is null, ideally it should not happen as null variableValueMap will not be emitted from parsingBolt for scoring at all*/
	@Test
	public void getModelIdListNullNewChangesVarValueMapTest1() {
		Map<String, String> newChangesVarValueMap = null;
		Set<Integer> modelList = scoringSingletonObj.getModelIdList(newChangesVarValueMap, null, null);
		assertTrue("expecting empty modelIdList as newChangesVarValueMap is null", modelList.isEmpty());
	}

	/*if none of the variables in newChangesVarValueMap is found in variableModelsMap, modelLists which needs to be scored will be empty*/
	@Test
	public void getModelIdListInvalidNewChangesVarValueMapTest2() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		Set<Integer> modelList = scoringSingletonObj.getModelIdList(newChangesVarValueMap, variableModelsMapContents, null);
		assertTrue("expecting empty modelIdList as none of the variables in newChangesVarValueMap is not in variableModelsMap", modelList.isEmpty());
	}

	/*if variableModelsMap does not contain any one of the variables from newChangesVarValueMap --
	 i.e. here variableModelsMap does not contain key S_DSL_APP_INT_ACC_FTWR_TRS2
	 The method is skipping that variable perfectly while populating modelIdLists which needs to be scored*/
	@Test
	public void getModelIdListForVariableNotPresentInVariableModelsMapTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(35);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR", modelLists2);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", new Variable("S_DSL_APP_INT_ACC_FTWR_TRS",0.002));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC_FTWR", new Variable("S_DSL_APP_INT_ACC_FTWR", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 5, variablesMap2));
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH), new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH), 5, variablesMap2));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		modelsMapContent.put(48, monthModelMap2);
		
		// Actual modelIds from ScoringSingleton
		Set<Integer> actualModelLists = scoringSingletonObj.getModelIdList(newChangesVarValueMap, variableModelsMapContents, modelsMapContent);
		// Expected modelIds
		Set<Integer> expectedModelLists = new HashSet<Integer>();
		expectedModelLists.add(48);
		Assert.assertEquals(expectedModelLists, actualModelLists);
	}
		
	/* This test is for a positive case, and return modelIdLists for newChangesVarValueMap
	 30 is currentMonth model and 48 is non month model*/
	@Test
	public void getModelIdListPositiveCaseTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {

		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(30);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists2);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", new Variable("S_DSL_APP_INT_ACC_FTWR_TRS",0.002));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC_FTWR_ALL", new Variable("S_DSL_APP_INT_ACC_FTWR_ALL", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(30, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 5, variablesMap2));
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH), new Model(30, "Model_Name", Calendar.getInstance().get(Calendar.MONTH), 5, variablesMap2));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(30, monthModelMap);
		modelsMapContent.put(48, monthModelMap2);
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_ALL", "1");

		// Actual modelIds from ScoringSingleton
		Set<Integer> actualModelList = scoringSingletonObj.getModelIdList(newChangesVarValueMap, variableModelsMapContents, modelsMapContent);
		// Expected modelIds
		Set<Integer> expectedModelLists = new HashSet<Integer>();
		expectedModelLists.add(48);
		expectedModelLists.add(30);
		Assert.assertEquals(expectedModelLists, actualModelList);
	}
	
	/*to test the filtration of the invalid month model
	here model 48 is not a current month model and not a non month model
	so, will not populated in modelIdList for scoring*/
	@Test
	public void getModelIdListForInvalidMonthModelTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {

		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", new Variable("S_DSL_APP_INT_ACC_FTWR_TRS",0.002));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(12-(Calendar.getInstance().get(Calendar.MONTH)), new Model(48, "Model_Name2", 12-(Calendar.getInstance().get(Calendar.MONTH)), 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(48, monthModelMap2);
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");

		// Actual modelIds from ScoringSingleton
		Set<Integer> actualModelList = scoringSingletonObj.getModelIdList(newChangesVarValueMap, variableModelsMapContents, modelsMapContent);
		Assert.assertTrue( actualModelList.isEmpty());
	}

	// This test is to check whether createMemberVariableValueMap() returns null if loyaltyid is null
	@Test
	public void createMemberVariableValueMapForNullLoyaltyIdTest()
			throws RealTimeScoringException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {

		Map<String, String> newChangesVarValueMap2 = new HashMap<String, String>();
		newChangesVarValueMap2.put("S_HOME_6M_IND2", "value");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		variableModelsMapContents.put("S_HOME_6M_IND2", modelLists);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND2", new Variable("S_HOME_6M_IND2",0.002));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND2", "2269");
		Set<Integer> modelIdList2 = scoringSingletonObj.getModelIdList(newChangesVarValueMap2, variableModelsMapContents, modelsMapContent);
		Map<String, Object> map = scoringSingletonObj.createMemberVariableValueMap("", modelIdList2, variableNameToVidMapContents, modelsMapContent);
		assertEquals("memberVariablesMap null as lid is null", map, null);
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
		memberVariables.insert(new BasicDBObject("l_id", lId).append("2269", 1).append("2270", 0.10455));
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(48, "Model_Name2", 0, 7, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		modelsMapContent.put(48, monthModelMap2);
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);
		modelIdsList.add(48);
		
		Map<String, Object> actualMemberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMapContents, modelsMapContent);
		Set<String> expMemVarValue = new HashSet<String>();
		expMemVarValue.add("2270");
		expMemVarValue.add("2269");
		Assert.assertEquals(expMemVarValue, actualMemberVariablesMap.keySet());
		memberVariables.remove(new BasicDBObject("l_id", lId));
	}
	
	/*to test the filteredVariables to fetch from memberVariables collection
	if variables associated with a modelId that is to be scored is not there in memberVariables collection,
	it wont be populated in the memberVariables map*/
	@Test
	public void createMemberVariableValueMapPositiveCaseTest2()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		String lId = "SearsTesting2";
		//fake memberVariables collection
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId).append("2269", 1).append("2270", 0.10455));
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2", 0.0915));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC3", new Variable("S_DSL_APP_INT_ACC3",0.002));
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
		Map<String, Object> actualMemberVariables = scoringSingletonObj.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMapContents, modelsMapContent);
		Set<String> expectedMemVarValue = new HashSet<String>();
		expectedMemVarValue.add("2270");
		expectedMemVarValue.add("2269");

		Assert.assertEquals("Expecting 2269 and 2270 only as 2271 not in memberVar collection", expectedMemVarValue, actualMemberVariables.keySet());
		memberVariables.remove(new BasicDBObject("l_id", lId));
	}
	
	/*model 48 is month model but does not correspond to current month, so the associated variables will not be fetched from memberVariables collection
	Ideally, this will not happen as those models will be filtered in getModelIdList() method itself*/
	@Test
	public void createMemberVariableValueMapInvalidMonthModelTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		String lId = "SearsTesting3";
		//fake memberVariables collection
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId).append("2269", 1).append("2270", 0.10455));
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",	0.002));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",	0.002));
		
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(12-(Calendar.getInstance().get(Calendar.MONTH)), new Model(48, "Model_Name2", 12-(Calendar.getInstance().get(Calendar.MONTH)), 7, variablesMap2));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		modelsMapContent.put(48, monthModelMap2);
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");	
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);
		modelIdsList.add(48);
		Map<String, Object> actualMemberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMapContents, modelsMapContent);
		Set<String> expectedMemVarValueMap = new HashSet<String>();
		expectedMemVarValueMap.add("2269");
		
		Assert.assertEquals("Expecting var associated with model 35 only as 48 is NOT current month model", expectedMemVarValueMap, actualMemberVariablesMap.keySet());
		
		memberVariables.remove(new BasicDBObject("l_id", lId));
	}

	/*if variable collection does not have a variable so that there is no VID in record, it will be skipped in the creation of memberVariableValueMap*/ 
	@Test
	public void createMemberVariableValueMapForNullVIDTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		
		String lId = "SearsTesting4";
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId).append("2269", 1).append("2268", 0.10455));

		//variablesMap, modelsMap are populated from modelVaraibles collection
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		//variableNameToVidMap is populated from variables collection
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);
		Map<String, Object> actualMemberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMapContents, modelsMapContent);
		Set<String> expectedMemVarValueMap = new HashSet<String>();
		expectedMemVarValueMap.add("2269");

		Assert.assertEquals("Expecting var S_DSL_APP_INT_ACC only as there is no VID for S_HOME_6M_IND", expectedMemVarValueMap, actualMemberVariablesMap.keySet());
		memberVariables.remove(new BasicDBObject("l_id", lId));
	}

	
	@Test
	public void createMemberVariableValueMapAllFilteredVarNotInMemVarCollTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		
		String lId = "SearsTesting44";
		DBCollection memberVariables = db.getCollection("memberVariables");
		memberVariables.insert(new BasicDBObject("l_id", lId).append("1000", 1).append("1001", 0.10455));

		//variablesMap, modelsMap are populated from modelVaraibles collection
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL", 0.0015));

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		//variableNameToVidMap is populated from variables collection
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		
		Set<Integer> modelIdsList = new HashSet<Integer>();
		modelIdsList.add(35);
		Map<String, Object> actualMemberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(lId, modelIdsList, variableNameToVidMapContents, modelsMapContent);
		Set<String> expectedMemVarValueMap = new HashSet<String>();
		
		Assert.assertEquals("Expecting empty memberVarValueMap as this member does not have any of the filtered variables", expectedMemVarValueMap, actualMemberVariablesMap.keySet());
		memberVariables.remove(new BasicDBObject("l_id", lId));
	}

	/* This test is to check whether changedMemberVariablesMap is getting populated
	 (positive case)*/
	@Test
	public void testCreateChangedMemberVariablesMap() throws ConfigurationException,
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
		Map<String, Change> changedVars = scoringSingletonObj.createChangedMemberVariablesMap(l_id, varIdToNameMapContents);
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
	}

	/* if member has all variables expired, they wont be populated in the map*/
	@Test
	public void testCreateChangedMemberVariablesWithExpVarTest() throws ConfigurationException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2270", 12,
				simpleDateFormat.parse("2014-10-21"),
				simpleDateFormat.parse("2014-10-01"));
	
		DBCollection changedMemberVar = db
				.getCollection("changedMemberVariables");
		String l_id = "6RpGnW1XhFFBoJV+T9cT9ok";

		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"2270",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		Map<String, String> varIdToNameMapContents = new HashMap<String, String>();
		varIdToNameMapContents.put("2270", "MY_VAR_NAME");
		Map<String, Change> changedVars = scoringSingletonObj.createChangedMemberVariablesMap(l_id, varIdToNameMapContents);
		Assert.assertEquals("Expecting an empty map as variables are expired", new HashMap<String, Change>(), changedVars);
		changedMemberVar.remove(new BasicDBObject("l_id",l_id));
	}
	
	/*if the member does not have any record in changedMemVar collection, changedMemVarMap will be empty*/
	@Test
	public void testCreateChangedMemberWithNullChangedMemVarTest() throws ConfigurationException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, ParseException {
		String l_id = "6RpGnW1XhFFBoJV+T9cT9ok2";
		Map<String, String> varIdToNameMapContents = new HashMap<String, String>();
		varIdToNameMapContents.put("2270", "MY_VAR_NAME");
		Map<String, Change> changedVars = scoringSingletonObj.createChangedMemberVariablesMap(l_id, varIdToNameMapContents);
		Assert.assertEquals("Expecting an empty map as there is no record in changedMemVar collection for this member", new HashMap<String, Change>(), changedVars);
	}
	
	//testing the boosting method with null allChanges
	@Test
	public void getBoostScoreNullAllChangesTest() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		double boost = scoringSingletonObj.getBoostScore(null, 35, modelsMapContent);
		int compare = (new Double(0.0)).compareTo(new Double(boost));
		Assert.assertEquals(compare, 0);
	}
	
	@Test
	public void getBoostScoreEmptyallChanges()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Variable("BOOST_S_DSL_APP_INT_ACC",0.0015));
		Map<Integer, Model> monthModelMap= new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name4", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals("Expecting boost value of 0", comapreVal, 0);
	}

	/*if the model does not have variables for this specific month -- null varMap*/
	@Test
	public void getBoostScoreWithNullVarMapForModelOfInterestTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, null));

		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals("Expecting a boost of 0 as there are no vars for this model", comapreVal, 0);
	}
	
	/*if the model does not have variables for this specific month -- with empty varMap */
	@Test
	public void getBoostScoreWithEmptyVarMapForModelOfInterestTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));

		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		int comapreVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals("Expecting a boost of 0 as there are no vars for this model", comapreVal, 0);
	}
	
	// This tests the boosts returned for current month model
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
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		int comapreVal = new Double(2.024).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
	}

 /* This test case is for testing the boost returned for non month modelId*/
	@Test
	public void getBoostScoreBoostVarPresentInModelsMapWithNonMonthModelTest()
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
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35, modelsMapContentBoost);
		int comapreVal = new Double(0.138).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
	}
	
	/*to test for boost variable which is NOT an BOOST instance, i.e. does not have an intercept and is just a Variable*/
	@Test
	public void getBoostScoreForNonBoostInstanceTest()
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
		variablesMap.put("BOOST_S_HOME_6M_IND", new Variable(
				"BOOST_S_HOME_6M_IND", 0.02));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35, modelsMapContentBoost);
		int comapreVal = new Double(0.124).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
	}
	
	// test for a non boost variable
	@Test
	public void getBoostScoreForNonBoostVariableTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",
				0.0015));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(27, "Model_Name2", 0, 5,
				variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		Assert.assertEquals("Expecting a boost of 0 as vars in allChanges are not BOOST variables", new Double(0.0), new Double(boost));
	}
	
	// to test a seasonal  model which does not belong to current month (Invalid month model)
		@Test
		public void getBoostScoreModelInvalidMonthModelTest()
				throws ParseException, SecurityException, NoSuchFieldException,
				IllegalArgumentException, IllegalAccessException {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
			Change change = new Change("2270", 12,
					simpleDateFormat.parse("2999-10-21"),
					simpleDateFormat.parse("2014-10-01"));
			HashMap<String, Change> allChanges = new HashMap<String, Change>();
			allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Boost("BOOST_S_DSL_APP_INT_ACC", 0.002, 0.1));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(12-(Calendar.getInstance().get(Calendar.MONTH)) , new Model(27, "Model_Name2", 12-(Calendar.getInstance().get(Calendar.MONTH)) , 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
			modelsMapContent.put(27, monthModelMap);
			double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
			Assert.assertEquals("Expecting a boost of 0 as the model doest not belong to current month", new Double(0.0), new Double(boost));
		}


	/*if a model is not in our modelVariables collection
	Ideally, this case will not happen at all*/
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
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Variable("BOOST_S_DSL_APP_INT_ACC",
				0.0015));
		Map<Integer, Model> monthModelMap= new HashMap<Integer, Model>();
		monthModelMap.put(
				Calendar.getInstance().get(Calendar.MONTH) + 1,
				new Model(27, "Model_Name4", Calendar.getInstance().get(
						Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(27, monthModelMap);
		double boost = scoringSingletonObj.getBoostScore(allChanges, 35, modelsMapContent);
		int compareVal = new Double(0.0).compareTo(new Double(boost));
		Assert.assertEquals("Expecting boost value of 0 as the model of interest is not in our modelVar collectio", compareVal, 0);
	}
	
	/*
	 * If a variable in allChanges is NOT in variables collection
	 * This can happen if changedMemVar collection has a variable, which got deleted in variables as well as modelVariables collection
	 * THIS CASE WAS IDENTIFIED IN PRODUCTION WITH EXCEPTION THROW, AND THE CODE WAS CHANGED TO HANDLE IT
	 * In this case, allChanges map is populated with null for VID 3270 as there is no record for 3270 in varaibles collection
	 */
	@Test
	public void getBoostScoreBoostWithNullVarNameInallChangesTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("3270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChanges.put(null, change2);

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
		double boost = scoringSingletonObj.getBoostScore(allChanges, 27, modelsMapContent);
		int comapreVal = new Double(2.024).compareTo(new Double(boost));
		Assert.assertEquals(comapreVal, 0);
	}

	//this test expects TRUE to be returned as allChanges for this member has blackout variable associated with model of interest
	@Test
	public void isBlackoutModelForBlackoutVarTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BLACKOUT_HA_COOK", change);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("BLACKOUT_HA_COOK", new Boost("BLACKOUT_HA_COOK", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBlackout = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBlackout.put(35, monthModelMap);
		Boolean value = scoringSingletonObj.isBlackOutModel(allChanges, 35, modelsMapContentBlackout).isBlackoutFlag();
		Assert.assertEquals(Boolean.TRUE, value);
	}

	//this test expects FALSE to be returned as the allChanges does not contain any blackout variable that is associated with the model
	@Test
	public void isBlackoutModelForNonBlackoutVarTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("HA_COOK", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("HA_COOK", new Boost("HA_COOK", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);
		Boolean value = scoringSingletonObj.isBlackOutModel(allChanges, 35, modelsMapContentBoost).isBlackoutFlag();
		Assert.assertEquals(Boolean.FALSE, value);
	}

	/*if the blackout variable is not in modelVariables collection for the model of interest*/
	@Test
	public void isBlackoutModelForBlackoutVarNotInModelVarMapTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("BLACKOUT_HA_COOK", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("HA_COOK", new Boost("HA_COOK", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);
		Boolean value = scoringSingletonObj.isBlackOutModel(allChanges, 35, modelsMapContentBoost).isBlackoutFlag();
		Assert.assertEquals(Boolean.FALSE, value);
	}
	
	/*
	 * If a variable in allChanges is NOT in variables collection
	 * This can happen if changedMemVar collection has a variable, which got deleted in variables as well as modelVariables collection
	 * THIS CASE WAS IDENTIFIED IN PRODUCTION WITH EXCEPTION THROW, AND THE CODE WAS CHANGED TO HANDLE IT
	 * In this case, allChanges map is populated with null for VID 3272 as there is no record for 3272 in varaibles collection
	 */
	@Test
	public void isBlackoutModelWithNullVarNameInallChangesTest()
			throws ParseException, SecurityException, NoSuchFieldException,
			IllegalArgumentException, IllegalAccessException {

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("3272", 1,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));

		HashMap<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put(null, change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("HA_COOK", new Boost("HA_COOK", 0.002, 0.1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();

		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5,variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMap);
		Boolean value = scoringSingletonObj.isBlackOutModel(allChanges, 35, modelsMapContentBoost).isBlackoutFlag();
		Assert.assertEquals(Boolean.FALSE, value);
	}
	//calc rtsScore positive case test
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
		
		double newScore = scoringSingletonObj.calcScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(0.9935028049029226).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	
	//test to check the rtsScore if baseScore>=35
	@Test
	public void calcScoreWithBaseScoreGT35Test() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ConfigurationException,
			RealTimeScoringException {
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 1.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				40));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		double newScore = scoringSingletonObj.calcScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(1.0).compareTo(new Double(newScore));
		Assert.assertEquals("Expecting a newScore of 1.0 as baseScore >= 35", comapreVal, 0);
	}

	//test to check the rtsScore if baseScore<-100
	@Test
	public void calcScoreWithBaseScoreLTNeg100() throws ParseException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ConfigurationException,
			RealTimeScoringException {
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 1.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL", 1));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, -105, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		double newScore = scoringSingletonObj.calcScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(0.0).compareTo(new Double(newScore));
		Assert.assertEquals("Expecting a newScore of 0.0 as baseScore is <= -100", comapreVal, 0);
	}
	
	/* If memberVariables is empty, was expected to throw RealTimeScoringException, now changed with the fact that scoring can be done with allChanges values
	 allChanges mentioned here is unexpired member variables..
	 Note: null memberVarMap is not checked as null memberVarMap was checked even before scoring part */
	@Test
	public void calcScoreForEmptyMemberVariablesTest() throws ParseException,
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
		double newScore = scoringSingletonObj.calcScore(new HashMap<String, Object>(), allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(0.9934277167211376).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
	}

	/* If allChanges is null, expected to throw RealTimeScoringException
	 ideally this should not happen*/
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForNullAllChangesTest() throws Exception,
			RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2271", 1);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		scoringSingletonObj.calcBaseScore(memVariables, null, 35, variableNameToVidMapContents, modelsMapContent);
		
	}
	
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForEmptyAllChangesTest() throws Exception,
			RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2271", 1);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		scoringSingletonObj.calcBaseScore(memVariables, new HashMap<String, Change>(), 35, variableNameToVidMapContents, modelsMapContent);
	}

	// tests the baseScore for non-month model
	@Test
	public void calcBaseScorePositiveCaseWithNonMonthModelTest() throws SecurityException,
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
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(5.0298663249999995).compareTo(new Double(baseScore));
		Assert.assertEquals(comapreVal, 0);
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
		
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(3.029866325).compareTo(new Double(baseScore));
		Assert.assertEquals(0, comapreVal);
	}
	
	/*to test a seasonal model which does not belong to current month (invalid month model), method throws exception, 
	which gets caught in the bolt and in api's execute method
	Ideally, this case will NOT happen at all as invalid month model gets filtered out in getModeIdList() method itself*/
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreInvalidMonthModelTest() throws SecurityException,
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
		monthModelMap.put(12-(Calendar.getInstance().get(Calendar.MONTH)) , new Model(35, "Model_Name", 12-(Calendar.getInstance().get(Calendar.MONTH)) , 3, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		
		scoringSingletonObj.calcBaseScore(memVariables,	allChanges, 35, variableNameToVidMapContents, modelsMapContent);
	}
	
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForNullVarMapforModelOfInterestTest() throws SecurityException,
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

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH)+1 , new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH)+1 , 3, null));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		scoringSingletonObj.calcBaseScore(memVariables,	allChanges, 35, null, modelsMapContent);
	}
	
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForEmptyVarMapforModelOfInterestTest() throws SecurityException,
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

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH)+1 , new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH)+1 , 3, new HashMap<String, Variable>()));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);		
		scoringSingletonObj.calcBaseScore(memVariables,	allChanges, 35, null, modelsMapContent);
	}
	
	/* tests the baseScore for null variable name or null VID
	 * if one var does not have proper record or does not have record at all in variables collection,
	 * that model is expected NOT TO BE SCORED
	 * in this case, model 35 has a variable with null varName
	 * */
	@Test(expected = RealTimeScoringException.class)
	public void calcBaseScoreForNullVarName() throws SecurityException,
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
		variablesMap.put(null, new Variable(null, 0.0915)); //variable name is passed as null
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		scoringSingletonObj.calcBaseScore(memVariables,	allChanges, 35, variableNameToVidMapContents, modelsMapContent);
	}
	
	//to test for a BOOST variable, the method skips the boost variable and scores with only MSM variables
	@Test
	public void calcBaseScoreWithOneBoostVar() throws SecurityException,
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
		variablesMap.put("BOOST_S_DSL_APP_INT_ACC", new Variable("BOOST_S_DSL_APP_INT_ACC", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("BOOST_S_DSL_APP_INT_ACC", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");

		double baseScore = scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(5.0203).compareTo(new Double(baseScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	//to test for a variable (S_DSL_APP_INT_ACC2) with no VID (i.e. variables collection does not contain it but is there in modelVaraibles collection)
	//expected to throw RealTimeScoringException
	@Test(expected=RealTimeScoringException.class)
	public void calcBaseScoreWithNoVIDVar() throws SecurityException,
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
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",0.0915));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
	}
	
	/*test to check the baseSScore with a variable NOT in memberVars as well as in changedMemVar
	the var will be skipped in scoring*/
	@Test
	public void calcBaseScoreWithVarNotInMbrVarAndallChanges() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
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
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");		
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(5.0203).compareTo(new Double(baseScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	/*test to check the baseSScore with a variable value (in memberVar or in allChanges) NOT an Integer nor a Double
	 * the variable will be skipped in scoring*/
	@Test
	public void calcBaseScoreWithVarValueNeitherIntNorDouble() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException, RealTimeScoringException {

		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", "1");
		memVariables.put("2271", 0.10455);

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC", 0.002));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL", 0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271");
		
		double baseScore = scoringSingletonObj.calcBaseScore(memVariables, allChanges, 35, variableNameToVidMapContents, modelsMapContent);
		int comapreVal = new Double(5.0183).compareTo(new Double(baseScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	//min max Expiry set by the expiration dates of the variables
	@Test
	public void getMinMaxExpWithTwoVarsTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2999-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2270", 0.2,	simpleDateFormat.parse("2888-10-21"), simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		allChanges.put("S_HOME_6M_IND_ALL2", change2);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL2", new Variable("S_HOME_6M_IND_ALL2",
				0.002));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL2", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(simpleDateFormat.parse("2888-10-21"), minMaxMap.get("minExpiry"));
		Assert.assertEquals(simpleDateFormat.parse("2999-10-21"), minMaxMap.get("maxExpiry"));
	}
	
	/*to test the min max exp with three variables
	minExp date set with earliest date among the expiration dates of the three variables
	maxExp date set with latest date among the expiration dates of the three variables*/
	@Test
	public void getMinMaxExpWithThreeVarsTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2270", 0.2, simpleDateFormat.parse("2888-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change3 = new Change("2269", 0.2, simpleDateFormat.parse("2999-10-21"), simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		allChanges.put("S_HOME_6M_IND_ALL2", change2);
		allChanges.put("S_HOME_6M_IND_ALL3", change3);
				
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_DSL_APP_INT_ACC",0.002));
		variablesMap.put("S_HOME_6M_IND_ALL2", new Variable("S_DSL_APP_INT_ACC2",0.002));
		variablesMap.put("S_HOME_6M_IND_ALL3", new Variable("S_DSL_APP_INT_ACC3",0.002));

		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL2", modelLists);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL3", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(simpleDateFormat.parse("2777-10-21"), minMaxMap.get("minExpiry"));
		Assert.assertEquals(simpleDateFormat.parse("2999-10-21"), minMaxMap.get("maxExpiry"));
	}
	
	/*variable is skipped if it is not in variableModelsMap (S_HOME_6M_IND_ALL in allChanges not in modelVar collection)*/
	@Test
	public void getMinMaxExpOneVarNotInVarModelsMapTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2270", 0.2, simpleDateFormat.parse("2888-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change3 = new Change("2269", 0.2, simpleDateFormat.parse("2999-10-21"), simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		allChanges.put("S_HOME_6M_IND_ALL2", change2);
		allChanges.put("S_HOME_6M_IND_ALL3", change3);
				
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL2", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL3", new Variable("S_DSL_APP_INT_ACC3",
				0.002));
		
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL2", modelLists);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL3", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(simpleDateFormat.parse("2888-10-21"), minMaxMap.get("minExpiry"));
		Assert.assertEquals(simpleDateFormat.parse("2999-10-21"), minMaxMap.get("maxExpiry"));
	}
	/*
	 * If all variables in allChanges associated for a model of interest is not variableModelsMap,
	 * min max exp will not be set for that model
	 * Ideally, this will not happen as models to be scored are picked from variableModelsMap only
	 */
	@Test
	public void getMinMaxExpAllAllChangesVarsNotInVarModelsMapTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
	
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL2", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
				
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH)+1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL2", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(null, minMaxMap.get("minExpiry"));
		Assert.assertEquals(null, minMaxMap.get("maxExpiry"));
	}
	
	/*if variables of interest are there in variableModelsMap but of none of them are associated with model of interest, then minDate, maxDate will be null
	Ideally, this should not happen when the flow reaches this point of code again, as models to b e scored are picked from variableModels map only
	testing as if an external class calling this method*/
	@Test
	public void getMinMaxExpForNonExistentCurrentMonthModelInVarModelsMapTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
		
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
				
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH)+1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH)+1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(null, minMaxMap.get("minExpiry"));
		Assert.assertEquals(null, minMaxMap.get("maxExpiry"));
	}
	
	/*
	 * Testing as an external class, this case will not happen
	 */
	@Test
	public void getMinMaxExpForNonExistentNonMonthModelInVarModelsMapTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
		
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
				
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(27, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(null, minMaxMap.get("minExpiry"));
		Assert.assertEquals(null, minMaxMap.get("maxExpiry"));
	}
	/*
	 * to test a month model whose variables' expiration dates are with the lastDateOfMonth
	 * so, not needed to explicitly set the dates with the lastDayOfMonth
	 */
	@Test
	public void getMinMaxExpForMonthModelWithValidMinDateTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
		Date lastDayOfMonth = calendar.getTime();
		Change change = new Change("2271", 0.2,	lastDayOfMonth, simpleDateFormat.parse("2014-10-01"));
		
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
				
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH)+1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH)+1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth), simpleDateFormat.format(minMaxMap.get("minExpiry")));
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth), simpleDateFormat.format(minMaxMap.get("maxExpiry")));
	}
	
	//to test whether minExpiry is set with lastDayOfMonth if it is a valid month model
	@Test
	public void getMinMaxExpForLastDayOfMonthTest() throws ParseException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2271", 0.2,	simpleDateFormat.parse("2777-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change2 = new Change("2270", 0.2, simpleDateFormat.parse("2888-10-21"), simpleDateFormat.parse("2014-10-01"));
		Change change3 = new Change("2269", 0.2, simpleDateFormat.parse("2999-10-21"), simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_HOME_6M_IND_ALL", change);
		allChanges.put("S_HOME_6M_IND_ALL2", change2);
		allChanges.put("S_HOME_6M_IND_ALL3", change3);
				
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_DSL_APP_INT_ACC",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL2", new Variable("S_DSL_APP_INT_ACC2",
				0.002));
		variablesMap.put("S_HOME_6M_IND_ALL3", new Variable("S_DSL_APP_INT_ACC3",
				0.002));
		
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(35, "Model_Name", Calendar.getInstance().get(Calendar.MONTH) + 1, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL2", modelLists);
		variableModelsMapContents.put("S_HOME_6M_IND_ALL3", modelLists);
		
		Map<String, Date> minMaxMap = scoringSingletonObj.getMinMaxExpiry(35, allChanges, variableModelsMapContents, modelsMapContent);
		
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));
		Date lastDayOfMonth = calendar.getTime();
		
		/*minExpiry is changed to string, as lastDayOfMonth gets changed by seconds before assertion and does not match with lastDayOfMonth
		returned by the method*/
		Assert.assertEquals(simpleDateFormat.format(lastDayOfMonth), simpleDateFormat.format(minMaxMap.get("minExpiry")));
		Assert.assertEquals(simpleDateFormat.parse("2999-10-21"), minMaxMap.get("maxExpiry"));
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
	public void strategyDCStrengthSumTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException{
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE14", "2000");
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(55);
		variableModelsMapContents.put("VARIABLE14", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE14", "StrategyDCStrengthSum");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE14", "14");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("13", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("14", 3.0,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE14", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(2003.0, allChangesMap.get("VARIABLE14").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(30).toDateMidnight().toDate()),allChangesMap.get("VARIABLE14").getExpirationDateAsString());
	}
	
	
	@Test
	public void strategyCountTraitsDatesTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException{
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE13", "{2999-08-24:[206634]}");
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(55);
		variableModelsMapContents.put("VARIABLE13", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE13", "StrategyCountTraitDates");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE13", "13");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("13", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("13", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE13", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(1, allChangesMap.get("VARIABLE13").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(1).toDateMidnight().toDate()),allChangesMap.get("VARIABLE13").getExpirationDateAsString());
	}
	
	@Test
	public void strategySumSalesTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE10", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE10", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE10", "StrategySumSales");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE10", "10");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("10", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("10", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE10", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(0.001, allChangesMap.get("VARIABLE10").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE10").getExpirationDateAsString());
	}
	
	/*if changedMemberVars is null or empty, previous value for allChanges will be set with value from memberVarMap*/
	@Test
	public void strategySumSalesEmptyChangedMemVarMapTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE10", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE10", modelLists);
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE10", "StrategySumSales");
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE10", "10");
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("10", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(new HashMap<String, Change>(), newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(0.001, allChangesMap.get("VARIABLE10").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE10").getExpirationDateAsString());
	}
	
	@Test
	public void strategySumSalesVarOFInterestNotInChangedMemVarMap() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE10", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE10", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE10", "StrategySumSales");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE10", "10");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("10", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("1", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE10", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(0.001, allChangesMap.get("VARIABLE10").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE10").getExpirationDateAsString());
	}
	
	@Test
	public void strategyCountTransactionsTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE1", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE1", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE1","StrategyCountTransactions");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE1", "1");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2269", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("1", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE1", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(4, allChangesMap.get("VARIABLE1").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE1").getExpirationDateAsString());
	}

	
	@Test
	public void strategyDaysSinceLastTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
			
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE4", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("VARIABLE4", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE4", "StrategyDaysSinceLast");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE4", "2284");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("4", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("4", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("variable4", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(1, allChangesMap.get("VARIABLE4").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE4").getExpirationDateAsString());
			
	}
	
	@Test
	public void strategyTurnOnFlagTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
	
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE5", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("VARIABLE5", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE5", "StrategyTurnOnFlag");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE5", "5");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("5", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("5", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE5", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(1, allChangesMap.get("VARIABLE5").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE5").getExpirationDateAsString());
	}

	@Test
	public void strategyTurnOffFlagTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE6", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("VARIABLE6", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE6", "StrategyTurnOffFlag");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE6", "6");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("6", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("6", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE6", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(0, allChangesMap.get("VARIABLE6").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()),allChangesMap.get("VARIABLE6").getExpirationDateAsString());
	}

	//ignored as there is no scoring for purchase occasions  
	@Ignore
	@Test
	public void strategyPurchaseOccasionsTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(35);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "StrategyPurchaseOccasions");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS2", "2283");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("2283", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("2283", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC_FTWR_TRS2", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals("0.001", allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(365).toDateMidnight().toDate()),allChangesMap.get("S_DSL_APP_INT_ACC_FTWR_TRS2").getExpirationDateAsString());
	}
	
	/*to test if all newChangesVar from incoming feed are not in varaibleModelsMap, allChanges map will be only unexpired member variables from changedMemVar collection
	 * ideally this will happen at this point, would have been taken care in modelIdsList itself*/
	@Test
	public void strategyIfAllNewChangesVarNotInVarModelsMapTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE12", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE1", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE1", "StrategyCountTransactions");
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE1", "1");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("1", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("1", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE1", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(3, allChangesMap.get("VARIABLE1").getValue());
		Assert.assertEquals("2999-10-21", allChangesMap.get("VARIABLE1").getExpirationDateAsString());
	}
	
	@Test
	public void strategyForVariableWithNONEStrategyTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE12", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE12", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE12", "NONE");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE12", "12");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("12", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("12", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE12", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(3, allChangesMap.get("VARIABLE12").getValue());
		Assert.assertEquals("2999-10-21", allChangesMap.get("VARIABLE12").getExpirationDateAsString());
	}

	/*If we pass unknown strategy to StrategyMapper class' getStrategy method, Strategy returned will be null
	 * Ideally, this will not happen at all as strategies in strategyMap (which is used in getStrategy method) and strategy in varNameToStrategy map 
	 * are populated from variables collection. Even the condition if (strategy == null) in executeStrategy is an over protective check 
	 * this test case is written just as an external class 
	 */
	@Test
	public void strategyForNullStrategyTest() throws SecurityException,
	NoSuchFieldException, IllegalArgumentException,
	IllegalAccessException, ParseException, ConfigurationException {
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("VARIABLE12", "0.001");
		
		Map<String, List<Integer>> variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		variableModelsMapContents.put("VARIABLE12", modelLists);
		
		Map<String, String> variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("VARIABLE12", "TESTING");
		
		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE12", "12");
		
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("12", 1);
		
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change change = new Change("12", 3,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE12", change);
		
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memVariables, variableNameToStrategyMapContents, variableNameToVidMapContents, variableModelsMapContents, null);
		Assert.assertEquals(3, allChangesMap.get("VARIABLE12").getValue());
		Assert.assertEquals("2999-10-21", allChangesMap.get("VARIABLE12").getExpirationDateAsString());
	}
	
	//This is a positive test case to update changedMemberScore collection
	@SuppressWarnings("unchecked")
	@Test
	public void updatechangedMemberScoreForUpdateTest(){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String lId = "SearsUpdate";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore();
		changedMemScore.setScore(0.02);
		changedMemScore.setMinDate("2014-09-10");
		changedMemScore.setMaxDate("2014-09-20");
		changedMemScore.setEffDate("2014-09-12");
		changedMemScore.setSource("test");
		
		changedMemberScore.insert(new BasicDBObject("l_id", lId)
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
			);
		
		List<ChangedMemberScore> changedMemberScoresList = new ArrayList<ChangedMemberScore>();
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore();
		changedMemScore2.setModelId("51");
		changedMemScore2.setScore(0.2);
		changedMemScore2.setMinDate("2014-10-10");
		changedMemScore2.setMaxDate("2999-09-20");
		changedMemScore2.setEffDate(dateFormat.format(new Date()));
		changedMemScore2.setSource("test");
		changedMemberScoresList.add(changedMemScore2);
		scoringSingletonObj.updateChangedMemberScore(lId, changedMemberScoresList, "test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", lId));
		HashMap<String, ChangedMemberScore> changedMemScoresUpdated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		Assert.assertEquals(changedMemScore2.getScore(), changedMemScoresUpdated.get("s"));
		Assert.assertEquals(changedMemScore2.getMinDate(), changedMemScoresUpdated.get("minEx"));
		Assert.assertEquals(changedMemScore2.getMaxDate(), changedMemScoresUpdated.get("maxEx"));
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	//to test update and insert; model 51 is updated and 48 is inserted
	@SuppressWarnings("unchecked")
	@Test
	public void updatechangedMemberScoreForInsertAndUpdateTest(){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String lId = "SearsUpdate2";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore();
		changedMemScore.setScore(0.02);
		changedMemScore.setMinDate("2014-09-10");
		changedMemScore.setMaxDate("2014-09-20");
		changedMemScore.setEffDate("2014-09-12");
		changedMemScore.setSource("test");
		
		changedMemberScore.insert(new BasicDBObject("l_id", lId)
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
			);
		
		List<ChangedMemberScore> changedMemberScoresList = new ArrayList<ChangedMemberScore>();
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore();
		changedMemScore2.setModelId("51");
		changedMemScore2.setScore(0.02);
		changedMemScore2.setMinDate("2014-10-10");
		changedMemScore2.setMaxDate("2999-09-20");
		changedMemScore2.setEffDate(dateFormat.format(new Date()));
		changedMemScore2.setSource("test");
		
		ChangedMemberScore changedMemScore3 = new ChangedMemberScore();
		changedMemScore3.setModelId("48");
		changedMemScore3.setScore(0.02);
		changedMemScore3.setMinDate("2013-10-10");
		changedMemScore3.setMaxDate("2888-09-20");
		changedMemScore3.setEffDate(dateFormat.format(new Date()));
		changedMemScore3.setSource("test");
		changedMemberScoresList.add(changedMemScore2);
		changedMemberScoresList.add(changedMemScore3);
		scoringSingletonObj.updateChangedMemberScore(lId, changedMemberScoresList, "test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", lId));
		HashMap<String, ChangedMemberScore> changedMemScores51Updated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		HashMap<String, ChangedMemberScore> changedMemScores48Updated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("48");
		
		Assert.assertEquals("model 51 updated", changedMemScore2.getScore(), changedMemScores51Updated.get("s"));
		Assert.assertEquals( "model 51 updated", changedMemScore2.getMinDate(), changedMemScores51Updated.get("minEx"));
		Assert.assertEquals("model 51 updated", changedMemScore2.getMaxDate(), changedMemScores51Updated.get("maxEx"));
		
		Assert.assertEquals( "model 48 inserted", changedMemScore3.getScore(), changedMemScores48Updated.get("s"));
		Assert.assertEquals("model 48 inserted", changedMemScore3.getMinDate(), changedMemScores48Updated.get("minEx"));
		Assert.assertEquals("model 48 inserted", changedMemScore3.getMaxDate(), changedMemScores48Updated.get("maxEx"));
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	//to test today's date set if min and max Exp dates are null
	@SuppressWarnings("unchecked")
	@Test
	public void updatechangedMemberScoreWithNullMinMAxDatesTest(){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String lId = "SearsUpdate3";
		DBCollection changedMemberScore = db.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore();
		changedMemScore.setScore(0.02);
		changedMemScore.setMinDate("2014-09-10");
		changedMemScore.setMaxDate("2014-09-20");
		changedMemScore.setEffDate("2014-09-12");
		changedMemScore.setSource("test");
		
		changedMemberScore.insert(new BasicDBObject("l_id", lId)
				.append("51",
						new BasicDBObject("s", changedMemScore.getScore())
								.append("minEx", changedMemScore.getMinDate())
								.append("maxEx", changedMemScore.getMaxDate())
								.append("f", changedMemScore.getEffDate()))
			);
		
		List<ChangedMemberScore> changedMemberScoresList = new ArrayList<ChangedMemberScore>();
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore();
		changedMemScore2.setModelId("51");
		changedMemScore2.setScore(0.02);
		changedMemScore2.setMinDate(null);
		changedMemScore2.setMaxDate(null);
		changedMemScore2.setEffDate(dateFormat.format(new Date()));
		changedMemScore2.setSource("test");
		changedMemberScoresList.add(changedMemScore2);
	
		scoringSingletonObj.updateChangedMemberScore(lId, changedMemberScoresList, "test");
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id", lId));
		HashMap<String, ChangedMemberScore> changedMemScores51Updated = (HashMap<String, ChangedMemberScore>) dbObj
				.get("51");
		
		
		Assert.assertEquals("model 51 updated", changedMemScore2.getScore(), changedMemScores51Updated.get("s"));
		Assert.assertEquals("model 51 updated with today as min Date", dateFormat.format(new Date()), changedMemScores51Updated.get("minEx"));
		Assert.assertEquals("model 51 updated with today as max Date", dateFormat.format(new Date()), changedMemScores51Updated.get("maxEx"));
		
		changedMemberScore.remove(new BasicDBObject("l_id", lId));
	}
	
	/*This is to check the update if all allChanges is null
	 The original values and dates of the existing changedMemberVar will be restored in the collection
	 Ideally it will not happen, was just checking as an external class*/
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesForNullAllChangesTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {
				
		String l_id = "SearsUpdate3";
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
	
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		scoringSingletonObj.updateChangedMemberVariables(l_id, null);
		DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id",
				l_id));
		HashMap<String, Object> map = (HashMap<String, Object>) dbObj
				.get("222");
		Double score = (Double) map.get("v");
		Assert.assertEquals(expected.getExpirationDateAsString(), map.get("e"));
		Assert.assertEquals(expected.getValue(), score);
		
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesForEmptyAllChangesTest()
			throws ConfigurationException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {
				
		String l_id = "SearsUpdate4";
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
	
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));

		scoringSingletonObj.updateChangedMemberVariables(l_id, new HashMap<String, Change>());
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
	//Anurag - Cache Refactoring - Needs to 
	@SuppressWarnings("unchecked")
	@Test
	@Ignore
	public void updateChangedVariablesForUpdateAndInsertTest() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException, ParseException {

		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		String l_id = "Example";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12.0,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
	
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append("222",
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
		
		Field variableNameToVidMap = ScoringSingleton.class.getDeclaredField("variableNameToVidMap");
		HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("MY_VAR_NAME", "222");
		variableNameToVidMapContents.put("MY_VAR_NAME2", "333");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		
		DBObject dbObject2 = changedMemberVar.findOne(new BasicDBObject("l_id", l_id));
			
		System.out.println("changedMemberVar before update" + dbObject2);

		
		scoringSingletonObj.updateChangedMemberVariables(l_id,allchanges);

		DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id",l_id));
			
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
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"TN", 0.2);
		Double factor = scoringSingletonObj.calcRegionalFactor(35, "TN", regionalFactorsMapContents);
		Assert.assertEquals(0.2, factor);
	}

	@Test
	public void calcRegionalFactorWithEmptyRegionalFactorTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		Double factor = scoringSingletonObj.calcRegionalFactor( 35, "TN", regionalFactorsMapContents);
		Assert.assertEquals(1.0, factor);
	}

	@Test
	public void calcRegionalWithNoRequiredModelIdTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"TN", 0.2);
		Double factor = scoringSingletonObj.calcRegionalFactor( 46, "TN", regionalFactorsMapContents);
		Assert.assertEquals(1.0, factor);
	}

	@Test
	public void calcRegionalWithNoRequiredStateTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Map<String, Double> regionalFactorsMapContents = new HashMap<String, Double>();
		regionalFactorsMapContents.put("35"+"-"+"IL", 0.2);
		Double factor = scoringSingletonObj.calcRegionalFactor( 35, "TN", regionalFactorsMapContents);
		Assert.assertEquals(1.0, factor);
	}

	@Test
	public void calcRegionalWithNoStateForMemberTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Double factor = scoringSingletonObj.calcRegionalFactor(35, null, null);
		Assert.assertEquals(1.0, factor);
	}
	
	@Test
	public void calcRegionalWithRegionalFactorMapTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException{
		Double factor = scoringSingletonObj.calcRegionalFactor( 35, "TN", null);
		Assert.assertEquals(1.0, factor);
	}
	
	//a positive case
	/*@Test
	public void getStateTest(){
		String l_id = "TestingLid";
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y").append("st_cd", "TN"));
		String state = scoringSingletonObj.getState(l_id);
		Assert.assertEquals("TN", state );
	}
	
	@Test
	public void getStateWithNoStateForMemberTest(){
		String l_id = "TestingLid2";
		//Fake memberInfo collection
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y"));
		String state = scoringSingletonObj.getState(l_id);
		Assert.assertEquals("Expecting null as state as there is no state field for this memebr", null, state );
	}
	
	@Test
	public void getStateWithNullMemberInfoTest(){
		String l_id = "TestingLid3";
		String state = scoringSingletonObj.getState(l_id);
		Assert.assertEquals("Expecting null as state as there is no record for this member in memberInfo coll", null, state );
	}*/
	
	@Test
	public void getDateFormatTest(){
		Date date = new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate();
		String dateReturned = scoringSingletonObj.getDateFormat(date);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		String expDate = simpleDateFormat.format(date);
		Assert.assertEquals(expDate, dateReturned);
	}
	
	@Test
	public void getDateFormatWithNullDateTest(){
		String dateReturned = scoringSingletonObj.getDateFormat(null);
		Assert.assertEquals(null, dateReturned);
	}
	
	@Test
	public void finalScoreWithRegionalFactorsWithNoMemberZipTest() throws RealTimeScoringException, ParseException{
		String lId = "scoringTestLid4";
		Map<String, Object> memVariables = new HashMap<String, Object>();
		memVariables.put("variable100", 0.10455);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("100", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Map<String, Change> allChanges = new HashMap<String, Change>();
		allChanges.put("VARIABLE100", change);

		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("VARIABLE100", new Variable("VARIABLE100",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(100, "Model_Name100", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(100, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE100", "100");
		
		Map<String, RegionalFactor> modelSeasonalZipMap = new HashMap<String, RegionalFactor>();
		RegionalFactor regFactor = new RegionalFactor();
		regFactor.setZip("99000");
		regFactor.setF_date(simpleDateFormat.format(new Date()));
		regFactor.setFactor(0.03);
		modelSeasonalZipMap.put("100", regFactor);
		
		Map<Integer, RegionalFactor> modelSeasonalNationalMap = new HashMap<Integer, RegionalFactor>();
		modelSeasonalNationalMap.put(100, regFactor);
		
		Map<Integer, Model> modelSeasonalConstantMap = new HashMap<Integer, Model>();
		Model model = new Model(100, "Model_Name100", "Model_Code", 0.025);
		modelSeasonalConstantMap.put(100, model);
		double newScore = scoringSingletonObj.finalScore(0.03, lId, 100, modelsMapContent, modelSeasonalZipMap, modelSeasonalNationalMap, modelSeasonalConstantMap);
		System.out.println(newScore);
		int comapreVal = new Double(0.03596311475409834).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	@Test
	public void calcScoreWithRegionalFactorsWithMemberSrsZipTest() throws RealTimeScoringException, ParseException{
		String lId = "scoringTestLid5";
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", lId).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y"));
	
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("VARIABLE100", new Variable("VARIABLE100",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(101, "S_SCR_TEST", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(101, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE100", "100");
		
		Map<String, RegionalFactor> modelSeasonalZipMap = new HashMap<String, RegionalFactor>();
		RegionalFactor regFactor = new RegionalFactor();
		regFactor.setZip("46142");
		regFactor.setF_date(simpleDateFormat.format(new Date()));
		regFactor.setFactor(0.03);
		modelSeasonalZipMap.put("101"+regFactor.getZip(), regFactor);
		
		Map<Integer, RegionalFactor> modelSeasonalNationalMap = new HashMap<Integer, RegionalFactor>();
		modelSeasonalNationalMap.put(101, regFactor);
		
		Map<Integer, Model> modelSeasonalConstantMap = new HashMap<Integer, Model>();
		Model model = new Model(101, "S_SCR_TEST", "Model_Code", 0.025);
		modelSeasonalConstantMap.put(101, model);
		double newScore = scoringSingletonObj.finalScore(0.03, lId, 101, modelsMapContent, modelSeasonalZipMap, modelSeasonalNationalMap, modelSeasonalConstantMap);
		System.out.println(newScore);
		int comapreVal = new Double(0.03596311475409834).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	@Test
	public void calcScoreWithRegionalFactorsWithMemberKmtZipTest() throws RealTimeScoringException, ParseException{
		String lId = "scoringTestLid5";
		DBCollection memInfoColl = db.getCollection("memberInfo");
		memInfoColl.insert(new BasicDBObject("l_id", lId).append("srs", "0001470")
				.append("srs_zip", "46142").append("kmt", "3251").append("kmt_zip", "46241")
				.append( "eid", "258003809").append("eml_opt_in", "Y"));
	
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("VARIABLE100", new Variable("VARIABLE100",
				0.0915));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(101, "K_SCR_TEST", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(102, monthModelMap);

		Map<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("VARIABLE100", "100");
		
		Map<String, RegionalFactor> modelSeasonalZipMap = new HashMap<String, RegionalFactor>();
		RegionalFactor regFactor = new RegionalFactor();
		regFactor.setZip("46241");
		regFactor.setF_date(simpleDateFormat.format(new Date()));
		regFactor.setFactor(0.5);
		modelSeasonalZipMap.put("102"+regFactor.getZip(), regFactor);
		
		Map<Integer, RegionalFactor> modelSeasonalNationalMap = new HashMap<Integer, RegionalFactor>();
		modelSeasonalNationalMap.put(102, regFactor);
		
		Map<Integer, Model> modelSeasonalConstantMap = new HashMap<Integer, Model>();
		Model model = new Model(102, "K_SCR_TEST", "Model_Code", 0.025);
		modelSeasonalConstantMap.put(102, model);
		double newScore = scoringSingletonObj.finalScore(0.3, lId, 102, modelsMapContent, modelSeasonalZipMap, modelSeasonalNationalMap, modelSeasonalConstantMap);
		System.out.println(newScore);
		int comapreVal = new Double(0.9435483870967741).compareTo(new Double(newScore));
		Assert.assertEquals(comapreVal, 0);
	}
	
	@AfterClass
	public static void cleanUp(){
		SystemPropertyUtility.dropDatabase();
	}
}
