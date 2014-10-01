package analytics.util;

import static org.junit.Assert.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import analytics.util.objects.Change;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ScoringSingletonTest {
	private static ScoringSingleton scoringSingletonObj;
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		System.setProperty("rtseprod", "test");	
		//Below line ensures an empty DB rather than reusing a DB with values in it
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		//We do not need instance of scoring singleton created by previous tests. If different methods need different instances, move this to @Before rather than before class
		Constructor<ScoringSingleton> constructor= (Constructor<ScoringSingleton>) ScoringSingleton.class.getDeclaredConstructors()[0];
		constructor.setAccessible(true); 
		scoringSingletonObj = constructor.newInstance(); 
	}

	@Before
	public void setUp() throws Exception {		
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testCreateChangedVariablesMap() throws ConfigurationException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, ParseException{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2270",12,simpleDateFormat.parse("2999-10-21"),simpleDateFormat.parse("2014-10-01"));
		DB conn = DBConnection.getDBConnection();
		DBCollection changedMemberVar = conn.getCollection("changedMemberVariables");
		String l_id = "6RpGnW1XhFFBoJV+T9cT9ok=";
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append("2270", new BasicDBObject("v",expected.getValue()).append("e", expected.getExpirationDateAsString()).append("f", expected.getEffectiveDateAsString())));
		
		Map<String,String> varIdToNameMapContents = new HashMap<String, String>();
		varIdToNameMapContents.put("2270","MY_VAR_NAME");
		Field varIdToNameMap = ScoringSingleton.class.getDeclaredField("variableVidToNameMap");
		varIdToNameMap.setAccessible(true);
		varIdToNameMap.set(scoringSingletonObj, varIdToNameMapContents);
        
		Map<String,Change> changedVars = scoringSingletonObj.createChangedVariablesMap(l_id);
		Assert.assertTrue(changedVars.containsKey("MY_VAR_NAME"));
		Change actual = changedVars.get("MY_VAR_NAME");
		Assert.assertEquals(expected.getValue(), actual.getValue());
		Assert.assertEquals(expected.getEffectiveDateAsString(), actual.getEffectiveDateAsString());
		Assert.assertEquals(expected.getExpirationDate(), actual.getExpirationDate());
		Assert.assertEquals(expected.getChangeVariable(), actual.getChangeVariable());
	}


	@Test
	public void getModelIdListTestNull1() {
		Map<String, String> newChangesVarValueMap = null;
		
		Set<Integer> modelList=  scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}
	
	@Test
	public void getModelIdListTest2(){
		//no access to variableModelsMap, need to fake data, added a public method, might not be best practice
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		//testTarget.setVariableModelsMap(null);
		Set<Integer> modelList=  scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	
	}
	
	@Test
	@Ignore("Populate fake mongo with values before expecting values")
	public void getModelIdListTest3(){
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_HOME_6M_IND", "value");
		//Map<String, List<Integer>> data = new HashMap("S_HOME_6M_IND",)
		Set<Integer> modelList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Set<Integer> result = new HashSet<Integer>();
		result.add(48);
		//System.out.println(modelList.toString());
		assertEquals(modelList, result);
	}
	
	@Test
	public void createVariableValueMapTest(){
		//TODO:
		//in condition mbrVariables is null, assertNull
		//variables key is not loyal_ID and is not ID(what are those constants?)
		//insert into memberVariablesMap
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_HOME_6M_IND", "value");
		Set<Integer> modelIdList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> map = scoringSingletonObj.createVariableValueMap("", modelIdList);
		assertEquals(map, null);//invalid l_id
	}
	
	@Test
	public void createChangedVariablesMapTest(){
		
	}
	
	@Test
	public void executeStrategyTest(){
		
	}
	
	@Test
	public void getBoostScoreTest(){
		//need the actual algorithm used in this method and give input values and expected outputs
	}
	
	@Test
	public void calcScoreTest(){
		//Feed in test data, and make assertion to the right score
		//test data needs to cover all cases that could happen calcBaseScore
		//testTarget.calcBaseScore(mbrVarMap, varChangeMap, modelId)
		//assertEquals(,)
	}
	
	@Test
	public void  calcBaseScoreTest(){
		//Feed in test data, and make assertion to the right score
		//test data needs to cover all cases that could happen calcBaseScore
		//testTarget.calcBaseScore(mbrVarMap, varChangeMap, modelId)
		//assertEquals(,)
	}
	
	@Test
	public void updateChangedMemberScoreTest(){
		//TODO: avoid making DB connection upon declaring the object
		//or connect only to FONGO when a test object is made
	}
	
	@Test
	public void updateChangedVariablesTest(){
		
	}

}
