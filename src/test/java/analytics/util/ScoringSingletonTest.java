package analytics.util;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class ScoringSingletonTest {
	private static ScoringSingleton scoringSingletonObj;

	Map<String, List<Integer>> variableModelsMapContents = null;
		
	Map<String, String> newChangesVarValueMap = null;
	HashMap<String, Object> memVariables= null;
	HashMap<String, Change> allChanges = null;
	Map<Integer, Map<Integer, Model>> modelsMapContent= null;
	Map<Integer, Map<Integer, Model>> modelsMapContent2= null;
	Map<Integer, Map<Integer, Model>> modelsMapContent3= null;
	
	HashMap<String, Change> allChangesBoost = null;
	Map<Integer, Map<Integer, Model>> modelsMapContentBoost = null;
	Map<Integer, Map<Integer, Model>> modelsMapContentBoost2 = null;
	Map<Integer, Map<Integer, Model>> modelsMapContentBoost3 = null;
	Map<Integer, Map<Integer, Model>> modelsMapContentBoost4 = null;
	Map<Integer, Map<Integer, Model>> modelsMapContentBoost5 = null;
	
	Map<String, String> variableNameToStrategyMapContents = null;
	Map<String, String> variableNameToVidMapContents = null;

	HashMap<String, Change> allChangesSywBoost = null;

	static DBCollection changedMemberVar;
	static DBCollection memberBoosts;
	static DBCollection modelSywBoosts;
	static DBCollection changedMemberScore;
	static DBCollection variables;
	
	Variable variable = null;
	
	public Map<String, String> getVariableNameToStrategyMapContents() {
		return variableNameToStrategyMapContents;
	}

	public void setVariableNameToStrategyMapContents(
			Map<String, String> variableNameToStrategyMapContents) {
		this.variableNameToStrategyMapContents = variableNameToStrategyMapContents;
	}


	public Map<String, String> getVariableNameToVidMapContents() {
		return variableNameToVidMapContents;
	}

	public void setVariableNameToVidMapContents(
			Map<String, String> variableNameToVidMapContents) {
		this.variableNameToVidMapContents = variableNameToVidMapContents;
	}
		
	
	public Map<Integer, Map<Integer, Model>> getModelsMapContent3() {
		return modelsMapContent3;
	}

	public void setModelsMapContent3(
			Map<Integer, Map<Integer, Model>> modelsMapContent3) {
		this.modelsMapContent3 = modelsMapContent3;
	}
	
	public Map<Integer, Map<Integer, Model>> getModelsMapContent2() {
		return modelsMapContent2;
	}

	public void setModelsMapContent2(
			Map<Integer, Map<Integer, Model>> modelsMapContent2) {
		this.modelsMapContent2 = modelsMapContent2;
	}
	
	public Map<Integer, Map<Integer, Model>> getModelsMapContentBoost4() {
		return modelsMapContentBoost4;
	}

	public void setModelsMapContentBoost4(
			Map<Integer, Map<Integer, Model>> modelsMapContentBoost4) {
		this.modelsMapContentBoost4 = modelsMapContentBoost4;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContentBoost3() {
		return modelsMapContentBoost3;
	}

	public void setModelsMapContentBoost3(
			Map<Integer, Map<Integer, Model>> modelsMapContentBoost3) {
		this.modelsMapContentBoost3 = modelsMapContentBoost3;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContentBoost2() {
		return modelsMapContentBoost2;
	}

	public void setModelsMapContentBoost2(
			Map<Integer, Map<Integer, Model>> modelsMapContentBoost2) {
		this.modelsMapContentBoost2 = modelsMapContentBoost2;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContentBoost() {
		return modelsMapContentBoost;
	}

	public void setModelsMapContentBoost(
			Map<Integer, Map<Integer, Model>> modelsMapContentBoost) {
		this.modelsMapContentBoost = modelsMapContentBoost;
	}

	public HashMap<String, Change> getAllChangesBoost() {
		return allChangesBoost;
	}

	public void setAllChangesBoost(HashMap<String, Change> allChangesBoost) {
		this.allChangesBoost = allChangesBoost;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContent() {
		return modelsMapContent;
	}

	public void setModelsMapContent(
			Map<Integer, Map<Integer, Model>> modelsMapContent) {
		this.modelsMapContent = modelsMapContent;
	}

	public HashMap<String, Change> getAllChanges() {
		return allChanges;
	}

	public void setAllChanges(HashMap<String, Change> allChanges) {
		this.allChanges = allChanges;
	}

	public HashMap<String, Object> getMemVariables() {
		return memVariables;
	}

	public void setMemVariables(HashMap<String, Object> memVariables) {
		this.memVariables = memVariables;
	}

	public Map<String, String> getNewChangesVarValueMap() {
		return newChangesVarValueMap;
	}

	public void setNewChangesVarValueMap(Map<String, String> newChangesVarValueMap) {
		this.newChangesVarValueMap = newChangesVarValueMap;
	}

	public Map<String, List<Integer>> getVariableModelsMapContents() {
		return variableModelsMapContents;
	}

	public void setVariableModelsMapContents(
			Map<String, List<Integer>> variableModelsMapContents) {
		this.variableModelsMapContents = variableModelsMapContents;
	}
	
	
	public HashMap<String, Change> getAllChangesSywBoost() {
		return allChangesSywBoost;
	}

	public void setAllChangesSywBoost(HashMap<String, Change> allChangesSywBoost) {
		this.allChangesSywBoost = allChangesSywBoost;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContentBoost5() {
		return modelsMapContentBoost5;
	}

	public void setModelsMapContentBoost5(
			Map<Integer, Map<Integer, Model>> modelsMapContentBoost5) {
		this.modelsMapContentBoost5 = modelsMapContentBoost5;
	}

	

	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException {
		System.setProperty("rtseprod", "test");
		// Below line ensures an empty DB rather than reusing a DB with values
		// in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		// We do not need instance of scoring singleton created by previous
		// tests. If different methods need different instances, move this to
		// @Before rather than before class
		Constructor<ScoringSingleton> constructor = (Constructor<ScoringSingleton>) ScoringSingleton.class
				.getDeclaredConstructors()[0];
		constructor.setAccessible(true);
		scoringSingletonObj = constructor.newInstance();
		
		//fake changedMemberVariables Collection
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("222", 12,
				simpleDateFormat.parse("2014-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		DB conn = DBConnection.getDBConnection();
		System.out.println("database: " + conn);
		changedMemberVar = conn
				.getCollection("changedMemberVariables");
		changedMemberVar.remove(new BasicDBObject("l_id","Sears"));
		String l_id = "Sears";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"222",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())));
		
		
		//Fake memberBoosts collection
		memberBoosts = conn.getCollection("memberBoosts");
		BasicDBList list = new BasicDBList();
		list.add("1");
		BasicDBObject dbObject2 = new BasicDBObject("BOOST_SYW_WANT_TOYS_TCOUNT",new BasicDBObject("2014-10-01",list)).append("BOOST_SYW_WANT_TOYS_TCOUNT2", new BasicDBObject("2014-10-04",list));
		BasicDBObject dbObject = new BasicDBObject("l_id", "Sears").append("boosts" , dbObject2);
		memberBoosts.insert(dbObject);
			
		//Fake modelSywBoosts collection
		modelSywBoosts = conn.getCollection("modelSywBoosts");
		modelSywBoosts.insert(new BasicDBObject("m",35).append("b", "BOOST_SYW_WANT_TOYS_TCOUNT"));
		modelSywBoosts.insert(new BasicDBObject("m",48).append("b", "BOOST_SYW_WANT_TOYS_TCOUNT2"));
		
		//Fake changedMemberScore collection
		changedMemberScore = conn.getCollection("changedMemberScores");
		ChangedMemberScore changedMemScore = new ChangedMemberScore(0.02, "2014-09-10", "2014-09-20", "2014-10-04");
		ChangedMemberScore changedMemScore2 = new ChangedMemberScore(0.102, "2014-08-10", "2014-08-20", "2014-10-04");
		changedMemberScore.insert(new BasicDBObject("l_id","Sears").append("51",new BasicDBObject("s",changedMemScore.getScore()).append("minEx",changedMemScore.getMinDate()).append("maxEx",changedMemScore.getMaxDate()).append("f",changedMemScore.getEffDate())).append("46", new BasicDBObject("s",changedMemScore2.getScore()).append("minEx",changedMemScore2.getMinDate()).append("maxEx", changedMemScore2.getMaxDate()).append("f",changedMemScore2.getEffDate())));
	
	}

	@Before
	public void setUp() throws Exception {

		//variablemodelsMap 
		variableModelsMapContents = new HashMap<String, List<Integer>>();
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		modelLists.add(35);
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(46);
		modelLists2.add(30);
		List<Integer> modelLists3 = new ArrayList<Integer>();
		modelLists3.add(51);
		modelLists3.add(30);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
		variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR", modelLists2);
		variableModelsMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT", modelLists2);
		variableModelsMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT2", modelLists3);
		
		//varValueMap
		newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_TRS", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_ALL", "0.002");
		
		//memberVariablesMap
		memVariables = new HashMap<String, Object>();
		memVariables.put("S_DSL_APP_INT_ACC", 1);
		memVariables.put("S_HOME_6M_IND", 0.10455);
		memVariables.put("S_DSL_APP_INT_BOTH", 0.155);
		setMemVariables(memVariables);
		
		//varChangeMap
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd");
		Change change = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		allChanges = new HashMap<String, Change>();
		Change change2 = new Change("2271", 0.2,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		Change change3 = new Change("2272", 0.12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		allChanges = new HashMap<String, Change>();
		allChanges.put("S_DSL_APP_INT_ACC2", change);
		allChanges.put("S_HOME_6M_IND_ALL", change2);
		allChanges.put("S_DSL_APP_INT_BOTH", change3);
		setAllChanges(allChanges);
		
		//Variable
		variable = new Variable("S_DSL_APP_INT_ACC",0.002);

		//modelsMap
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",	0.002));
		variablesMap.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));
		variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",0.0915));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.0915));
		variablesMap.put("S_DSL_APP_INT_BOTH", new Variable("S_DSL_APP_INT_BOTH",0.0915));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_HOME_ALL", new Variable("S_HOME_ALL", 0.075));
		
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 11, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(27, "Model_Name2", 12, 3,variablesMap));
		Map<Integer, Model> monthModelMap3 = new HashMap<Integer, Model>();
		monthModelMap3.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(27, "Model_Name2", 12, 3,variablesMap2));

		
		modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent.put(35, monthModelMap);
		setModelsMapContent(modelsMapContent);
		
		modelsMapContent2 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent2.put(27, monthModelMap2);
		setModelsMapContent2(modelsMapContent2);
		
		modelsMapContent3 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContent3.put(27, monthModelMap3);
		setModelsMapContent3(modelsMapContent3);
		
		//varChangeMapBoost
		allChangesBoost = new HashMap<String, Change>();
		allChangesBoost.put("BOOST_S_DSL_APP_INT_ACC", change);
		allChangesBoost.put("BOOST_S_HOME_6M_IND", change2);
		setAllChangesBoost(allChangesBoost);
		
		//varChangeMapBoost
		 allChangesSywBoost = new HashMap<String, Change>();
		 allChangesSywBoost.put("BOOST_SYW_WANT_TOYS_TCOUNT", change);
		 allChangesSywBoost.put("BOOST_SYW_WANT_TOYS_TCOUNT2", change);
		setAllChangesSywBoost(allChangesSywBoost);
				
		
		//modelsMapForBoostVar
		Map<String, Variable> variablesMapBoost = new HashMap<String, Variable>();
		variablesMapBoost.put("BOOST_S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC", 0.002));
		Map<String, Variable> variablesMapBoost2 = new HashMap<String, Variable>();
		variablesMapBoost2.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));
		Map<String, Variable> variablesMapBoost3 = new HashMap<String, Variable>();
		variablesMapBoost3.put("BOOST_S_DSL_APP_INT_ACC", new Variable("BOOST_S_DSL_APP_INT_ACC", 0.002));
		Map<String, Variable> variablesMapBoost4 = new HashMap<String, Variable>();
		variablesMapBoost4.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));
		Map<String, Variable> variablesMapBoost5 = new HashMap<String, Variable>();
		variablesMapBoost5.put("BOOST_SYW_WANT_TOYS_TCOUNT", new Variable("BOOST_SYW_WANT_TOYS_TCOUNT", 0.0015));
		Map<String, Variable> variablesMapBoost6 = new HashMap<String, Variable>();
		variablesMapBoost6.put("BOOST_SYW_WANT_TOYS_TCOUNT", new Variable("BOOST_SYW_WANT_TOYS_TCOUNT2", 0.0015));
		
		Map<Integer, Model> monthModelMapBoost = new HashMap<Integer, Model>();
		monthModelMapBoost.put(0, new Model(35, "Model_Name", 11, 5, variablesMapBoost));
		Map<Integer, Model> monthModelMapBoost2 = new HashMap<Integer, Model>();
		monthModelMapBoost2.put(0, new Model(27, "Model_Name", 11, 5, variablesMapBoost2));
		Map<Integer, Model> monthModelMapBoost3 = new HashMap<Integer, Model>();
		monthModelMapBoost3.put(Calendar.getInstance().get(Calendar.MONTH) + 1,new Model(27, "Model_Name", 11, 5, variablesMapBoost3));
		Map<Integer, Model> monthModelMapBoost4 = new HashMap<Integer, Model>();
		monthModelMapBoost4.put(Calendar.getInstance().get(Calendar.MONTH) + 1,new Model(27, "Model_Name", 11, 5, variablesMapBoost4));
		Map<Integer, Model> monthModelMapBoost5 = new HashMap<Integer, Model>();
		monthModelMapBoost5.put(Calendar.getInstance().get(Calendar.MONTH) + 1,new Model(27, "Model_Name", 11, 5, variablesMapBoost5));
		
		modelsMapContentBoost = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost.put(35, monthModelMapBoost);
		setModelsMapContentBoost(modelsMapContentBoost);
		
		modelsMapContentBoost2 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost2.put(27, monthModelMapBoost2);
		setModelsMapContentBoost2(modelsMapContentBoost2);
		
		modelsMapContentBoost3 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost3.put(27, monthModelMapBoost3);
		setModelsMapContentBoost3(modelsMapContentBoost3);
		
		modelsMapContentBoost4 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost4.put(27, monthModelMapBoost4);
		setModelsMapContentBoost4(modelsMapContentBoost4);
		
		modelsMapContentBoost5 = new HashMap<Integer, Map<Integer, Model>>();
		modelsMapContentBoost5.put(51, monthModelMapBoost5);
		setModelsMapContentBoost5(modelsMapContentBoost5);
		
		variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC2", "NONE");
		variableNameToStrategyMapContents.put("S_HOME_6M_IND_ALL", "NONE");
			
	}
	
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testCreateChangedVariablesMap() throws ConfigurationException,
			NoSuchFieldException, SecurityException, IllegalArgumentException,
			IllegalAccessException, ParseException {
			
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2270", 12,
				simpleDateFormat.parse("2999-10-21"),
				simpleDateFormat.parse("2014-10-01"));
		DB conn = DBConnection.getDBConnection();
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
	public void getModelIdListTestNull1() {
		Map<String, String> newChangesVarValueMap = null;
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}

	@Test
	public void getModelIdListTest2() {
		// no access to variableModelsMap, need to fake data, added a public method, might not be best practice
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		// testTarget.setVariableModelsMap(null);
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}

	@Test
	public void getModelIdListTest3() throws ConfigurationException,
			SecurityException, NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, getVariableModelsMapContents());
		// Actual modelIds from ScoringSingleton
		Set<Integer> modelList = scoringSingletonObj
				.getModelIdList(getNewChangesVarValueMap());
		// Expected modelIds
		Set<Integer> result = new HashSet<Integer>();
		result.add(48);
		result.add(35);
		assertEquals(modelList, result);
	}

	@Test
	public void createVariableValueMapTest() {
		// TODO:
		// in condition mbrVariables is null, assertNull
		// variables key is not loyal_ID and is not ID(what are those
		// constants?)
		// insert into memberVariablesMap
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_HOME_6M_IND", "value");
		Set<Integer> modelIdList = scoringSingletonObj
				.getModelIdList(newChangesVarValueMap);
		Map<String, Object> map = scoringSingletonObj.createVariableValueMap(
				"", modelIdList);
		assertEquals(map, null);// invalid l_id
	}

	@Test
	public void getBoostScoreNullCheckTest() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		double boost = scoringSingletonObj.getBoostScore(null, null);
		Assert.assertEquals(0.0, boost);
	}
	
	@Test
	public void getBoostScoreTest() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
	
		//This test case is for general checking with boost var present in modelsMap with month 0 
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost());
		double boost = scoringSingletonObj.getBoostScore(getAllChangesBoost(), 35);
		Assert.assertEquals(0.024, boost);
	}

	@Test
	public void getBoostScoreTest2() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
	
		// If the modelsMap month is 0 but does not contain the boost variables
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost2());
		double boost = scoringSingletonObj.getBoostScore(getAllChangesBoost(), 27);
		Assert.assertEquals(0.0, boost);
	}

	@Test
	public void getBoostScoreTest3() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		// If the modelsMap month is current month and if it contains the boost variables
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost3());
		double boost = scoringSingletonObj.getBoostScore(getAllChangesBoost(), 27);
		Assert.assertEquals(0.024, boost);
	}

	@Test
	public void getBoostScoreTest4() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		// If the modelsMap month is current month and if it does not contain the boost variables
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost4());
		double boost = scoringSingletonObj.getBoostScore(getAllChangesBoost(), 27);
		Assert.assertEquals(0.0, boost);
	}
	

	/*@Test
	public void calcScoreTest() throws ParseException, SecurityException,
			NoSuchFieldException, IllegalArgumentException,
			IllegalAccessException {
		// This test case is tested with variables for model Id 35
		// neither changedvariables nor membervariables contain those model variables.
		// so, base score will be 0 and new score will be 0.5
		// it checks base score -- which will be zero
		
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_HOME_ALL", new Variable("S_HOME_ALL", 0.075));
		HashMap<Integer, Map<Integer, Model>> modelsMapContent3 = new HashMap<Integer, Map<Integer, Model>>();
		Map<Integer, Model> monthModelMap3 = new HashMap<Integer, Model>();
		monthModelMap3.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(27, "Model_Name2", 12, 3,variablesMap2));

		modelsMapContent3.put(27, monthModelMap3);
		
		Field modelsMap2 = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap2.setAccessible(true);
		modelsMap2.set(scoringSingletonObj, modelsMapContent3);

		double newScore = scoringSingletonObj.calcScore(getMemVariables(),getAllChanges(), 35);
		Assert.assertEquals(0.5, newScore);
	}*/
	
	@Test
	public void calcBaseScoreNullCheckTest() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
	
		//This test case is tested with null membervariables 
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent());
		double baseScore = scoringSingletonObj.calcBaseScore(null,getAllChanges(), 35);
		Assert.assertEquals(6.12728, baseScore);
	}
	
	//have to test calculateVariableValue() before this, but calculateVariableValue() is private method
	//so, one thing to check is why calculateVariableValue() has to return Object, why cant we convert it into double always
	//Problem is when we pass changes as null, calcBaseScore() throws null pointer exception
	//inorder to avoid that, if changes is null, calculateVariableValue() has to return 0.0
	//As val is cast to Integer, there arises the problem
	//This needs to be checked
	@Test
	public void calcBaseScoreNullCheckTest2() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
	
		//This test case is tested with null membervariables 
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent());
		double baseScore = scoringSingletonObj.calcBaseScore(getMemVariables(),null, 35);
		Assert.assertEquals(5.016339325000001, baseScore);
	}
	
	@Test
	public void calcBaseScoreNullCheckTest3() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
	
		//This test case is tested with null membervariables and null changes
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent());
		double baseScore = scoringSingletonObj.calcBaseScore(null,null, 35);
		Assert.assertEquals(5.0, baseScore);
	}
	
	@Test
	public void calcBaseScoreTest() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
	
		//This test case is tested with variables in membervariables or in varChanges, month is 0
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent());
		double baseScore = scoringSingletonObj.calcBaseScore(getMemVariables(),getAllChanges(), 35);
		Assert.assertEquals(6.129436825, baseScore);
	}

	@Test
	public void calcBaseScoreTest2() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
		//This test case is tested with variables in membervariables or in varChanges, month is current
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent2());
		double baseScore = scoringSingletonObj.calcBaseScore(getMemVariables(),getAllChanges(), 27);
		Assert.assertEquals(4.129436825 , baseScore);
	}

	@Test
	public void calcBaseScoreTest3() throws SecurityException,
			NoSuchFieldException, ParseException, IllegalArgumentException,
			IllegalAccessException {
		//This test case is tested with modelId whose month is current month and variables NOT in both membervariables and varchanges
		//so, it has to return the model Constant as base score
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContent3());
		double baseScore = scoringSingletonObj.calcBaseScore(getMemVariables(),getAllChanges(), 27);
		Assert.assertEquals(3.0, baseScore);
	}


	@Test
	public void executeStrategyTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		Field varaibleModelsMap = ScoringSingleton.class.getDeclaredField("variableModelsMap");
		varaibleModelsMap.setAccessible(true);
		varaibleModelsMap.set(scoringSingletonObj,getVariableModelsMapContents());

		Field variableNameToStrategyMap = ScoringSingleton.class.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj,getVariableNameToStrategyMapContents());

		Field variableNameToVidMap = ScoringSingleton.class.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,getVariableNameToVidMapContents());
		
		
	//	Map<String, Change> allChanges = scoringSingletonObj.executeStrategy(allChanges, newChangesVarValueMap, memberVariablesMap)

	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScoreNullCheckTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
				
		//HashMap map = (HashMap) changedMemberScore.findOne(new BasicDBObject("l_id","Sears"));
		//System.out.println("changedMemScore before update: " + map );
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, getVariableModelsMapContents());
		
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost5());
		
		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(35);
		modelIds.add(48);
		modelIds.add(51);
		modelIds.add(46);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		modelIdScoreMap.put(46, 0.012);
		scoringSingletonObj.updateChangedMemberScore("Sears", modelIds, getAllChangesSywBoost(), modelIdScoreMap);
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id","Sears"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore> ) dbObj.get("51");
		Map<String, ChangedMemberScore> changedMemScores46 = (HashMap<String, ChangedMemberScore> ) dbObj.get("46");
		Assert.assertEquals(0.09, changedMemScores51.get("s"));
		Assert.assertEquals(0.012, changedMemScores46.get("s"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedMemberScoreTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
				
		//HashMap map = (HashMap) changedMemberScore.findOne(new BasicDBObject("l_id","Sears"));
		//System.out.println("changedMemScore before update: " + map );
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, getVariableModelsMapContents());
		
		Field modelsMap = ScoringSingleton.class.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj, getModelsMapContentBoost5());
		
		Set<Integer> modelIds = new HashSet<Integer>();
		modelIds.add(35);
		modelIds.add(48);
		modelIds.add(51);
		modelIds.add(46);
		Map<Integer, Double> modelIdScoreMap = new HashMap<Integer, Double>();
		modelIdScoreMap.put(51, 0.09);
		modelIdScoreMap.put(46, 0.012);
		scoringSingletonObj.updateChangedMemberScore("Sears", modelIds, getAllChangesSywBoost(), modelIdScoreMap);
		DBObject dbObj = changedMemberScore.findOne(new BasicDBObject("l_id","Sears"));
		HashMap<String, ChangedMemberScore> changedMemScores51 = (HashMap<String, ChangedMemberScore> ) dbObj.get("51");
		Map<String, ChangedMemberScore> changedMemScores46 = (HashMap<String, ChangedMemberScore> ) dbObj.get("46");
		Assert.assertEquals(0.09, changedMemScores51.get("s"));
		Assert.assertEquals(0.012, changedMemScores46.get("s"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesNullCheckTest() throws ConfigurationException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException {
	scoringSingletonObj.updateChangedVariables("Sears", 35, null);
	DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id","Sears"));
	HashMap<String, Object> map = (HashMap<String, Object>) dbObj.get("222");
	Assert.assertEquals("2014-09-23", map.get("e"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesNullCheckTest2() throws ConfigurationException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException {
	scoringSingletonObj.updateChangedVariables("Sears2", null, null);
	DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id","Sears"));
	HashMap<String, Object> map = (HashMap<String, Object>) dbObj.get("222");
	Assert.assertEquals("2014-09-23", map.get("e"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesNullCheckTest3() throws ConfigurationException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException {
		//Looks  like modelId does not have any effect, why need of modelId as parameter for this method????
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Field variableNameToVidMap = ScoringSingleton.class.getDeclaredField("variableNameToVidMap");
		Change change = new Change("222",10, simpleDateFormat.parse("2888-11-20"), simpleDateFormat.parse("2014-10-04"));
		HashMap<String,Change> allVarchanges = new HashMap<String, Change>();
		allVarchanges.put("MY_VAR_NAME", change);
		HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("MY_VAR_NAME", "222");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
		scoringSingletonObj.updateChangedVariables("Sears", null, allVarchanges);
		DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id", "Sears"));
		HashMap<String, Object> var222Map = (HashMap<String, Object>) dbObject.get("222");
		Assert.assertEquals(change.getExpirationDateAsString(), var222Map.get("e"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateChangedVariablesTest() throws ConfigurationException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, ParseException {
	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	DBObject obj = changedMemberVar.findOne(new BasicDBObject("l_id", "Sears"));
	System.out.println("Before update sears: " + obj);
	//Testing update
	Change change = new Change("222",10, simpleDateFormat.parse("2888-11-20"), simpleDateFormat.parse("2014-10-04"));
	Change change2 = new Change("333",10, simpleDateFormat.parse("2999-11-20"), simpleDateFormat.parse("2014-10-04"));
	HashMap<String,Change> allVarchanges = new HashMap<String, Change>();
	allVarchanges.put("MY_VAR_NAME", change);
	allVarchanges.put("MY_VAR_NAME2", change2);
	Field variableNameToVidMap = ScoringSingleton.class.getDeclaredField("variableNameToVidMap");
	HashMap<String, String> variableNameToVidMapContents = new HashMap<String, String>();
	variableNameToVidMapContents.put("MY_VAR_NAME", "222");
	variableNameToVidMapContents.put("MY_VAR_NAME2", "333");
	variableNameToVidMap.setAccessible(true);
	variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContents);
	scoringSingletonObj.updateChangedVariables("Sears", 35, allVarchanges);
	//System.out.println("after update: " + changedMemberVar.findOne(new BasicDBObject("l_id", "Sears")));
	DBObject dbObject = changedMemberVar.findOne(new BasicDBObject("l_id", "Sears"));
	HashMap<String, Object> var222Map = (HashMap<String, Object> ) dbObject.get("222");
		
	//Testing insert
	scoringSingletonObj.updateChangedVariables("Sears2", 35, allVarchanges);
	DBObject dbObj = changedMemberVar.findOne(new BasicDBObject("l_id","Sears2"));
	HashMap<String, Object> var333Map = (HashMap<String, Object> ) dbObject.get("333");
	Assert.assertEquals(change2.getEffectiveDateAsString(), var333Map.get("f"));
	//System.out.println("Sears2: " + dbObj);
	
	Assert.assertEquals(change.getExpirationDateAsString(), var222Map.get("e"));
	
	//Expected size is 4 as it includes _id and l_id also
	Assert.assertEquals(4, dbObject.keySet().size());
	Assert.assertNotNull(dbObj);
	
	
	
	}

}
