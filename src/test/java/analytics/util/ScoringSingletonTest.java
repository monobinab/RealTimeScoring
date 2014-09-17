package analytics.util;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;

public class ScoringSingletonTest {
	ScoringSingleton testTarget;
	@Before
	public void setUp() throws Exception {
		//Fongo mockMongo = new Fongo("sample");
		testTarget = ScoringSingleton.getInstance();
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void getModelIdListTestNull1() {
		Map<String, String> newChangesVarValueMap = null;
		
		Set<Integer> modelList=  testTarget.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	}
	
	@Test
	public void getModelIdListTest2(){
		//no access to variableModelsMap, need to fake data, added a public method, might not be best practice
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("key", "value");
		//testTarget.setVariableModelsMap(null);
		Set<Integer> modelList=  testTarget.getModelIdList(newChangesVarValueMap);
		assertTrue(modelList.isEmpty());
	
	}
	
	@Test
	public void getModelIdListTest3(){
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_HOME_6M_IND", "value");
		//Map<String, List<Integer>> data = new HashMap("S_HOME_6M_IND",)
		Set<Integer> modelList = testTarget.getModelIdList(newChangesVarValueMap);
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
		Set<Integer> modelIdList = testTarget.getModelIdList(newChangesVarValueMap);
		Map<String, Object> map = testTarget.createVariableValueMap("", modelIdList);
		System.out.println(map.toString());
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
