package analytics.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.configuration.ConfigurationException;
import org.joda.time.LocalDate;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import analytics.exception.RealTimeScoringException;
import analytics.util.objects.Change;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ScoringSingletonIntegrationTest {
	private static ScoringSingleton scoringSingletonObj;

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void initializeFakeMongo() throws InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, ParseException, ConfigurationException {
		System.setProperty("rtseprod", "test");
		FakeMongo.setDBConn(new Fongo("test db").getDB("test"));
		
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

	//This integration test is check the re-scored value for modelIds 35  (a positive case)
	@Test
	public void executeScoringSingletonBasicPositiveCaseTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting").append("2269", 1).append("2270",0.4));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected2 = new Change("2270", 1,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString()).append(
										"2270",
										new BasicDBObject("v", expected2.getValue()).append("e",
												expected2.getExpirationDateAsString()).append("f",
												expected2.getEffectiveDateAsString())))
														);
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(35);
		
		
		Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists2);
		Field variableModelsMapContent = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMapContent.setAccessible(true);
		variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap2));
		Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
		modelsContentMap.put(48, monthModelMap);
		modelsContentMap.put(35, monthModelMap2);
		Field modelsMapContent = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMapContent.setAccessible(true);
		modelsMapContent.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
		Field variableNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMapContents.setAccessible(true);
		variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
		Field variableVidToNameMapContents = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMapContents.setAccessible(true);
		variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
		Field variableNameToStrategyMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMapContents.setAccessible(true);
		variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting", modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting");
		Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
		double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
		newScoreListActual.add(score);
		}
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.9934391327651371);
		newScoreListExpected.add(0.9934061356083235);
		Assert.assertEquals(newScoreListExpected, newScoreListActual);
	}

	//This test is check, if all the variables in changedMemberVariables are expired
	//newChangesVarValueMap, from parsing bolt which needs re-scoring is populated into changedMemberVariables map
	@Test
	public void executeScoringSingletonAllChangedMemberVariablesExpiredTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		//Fake memberVariables collection
				DB db = DBConnection.getDBConnection();
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", "SearsTesting2").append("2269", 1).append("2270",0.4));
				
						//fake changedMemberVariables Collection
						DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Change expected = new Change("2269", 12,
								simpleDateFormat.parse("2014-08-23"),
								simpleDateFormat.parse("2014-09-01"));
						Change expected2 = new Change("2270", 1,
								simpleDateFormat.parse("2014-08-23"),
								simpleDateFormat.parse("2014-09-01"));
						
						changedMemberVar = db
								.getCollection("changedMemberVariables");
						String l_id = "SearsTesting2";
						changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
								"2269",
								new BasicDBObject("v", expected.getValue()).append("e",
										expected.getExpirationDateAsString()).append("f",
										expected.getEffectiveDateAsString())).append(
												"2270",
												new BasicDBObject("v", expected2.getValue()).append("e",
														expected2.getExpirationDateAsString()).append("f",
														expected2.getEffectiveDateAsString()))
																);
							
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
				List<Integer> modelLists = new ArrayList<Integer>();
				modelLists.add(48);
				List<Integer> modelLists2 = new ArrayList<Integer>();
				modelLists2.add(35);
				
				Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists2);
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
			
				Map<String, Variable> variablesMap = new HashMap<String, Variable>();
				variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
				Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
				variablesMap2.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.01));
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
				Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
				monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap2));
				Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
				modelsContentMap.put(48, monthModelMap);
				modelsContentMap.put(35, monthModelMap2);
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting2", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting2");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
				double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
				newScoreListActual.add(score);
				}
			
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9934061356083235);
				Assert.assertEquals(2, changedMemberVariablesMap.size());
				Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
								Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
			//	Assert.assertEquals(newScoreListExpected, newScoreListActual);
		}
	
		
	//This test case is for testing variable S_DSL_APP_INT_ACC which is not expired in changedMemberVaribles and newchangeVarValueMap also contains it
	//The value for that variable will be set from executestrategy method 
	@Test
	public void executeScoringSingletonchangedMemberVariablesNotExpiredPresentinNewChangesVariableTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
		//Fake memberVariables collection
			
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", "SearsTesting3").append("2269", 1).append("2270",0.4));
				
						//fake changedMemberVariables Collection
						DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Change expected = new Change("2269", 12,
								simpleDateFormat.parse("2999-09-23"),
								simpleDateFormat.parse("2014-09-01"));
					
						changedMemberVar = db
								.getCollection("changedMemberVariables");
						String l_id = "SearsTesting3";
						changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
								"2269",
								new BasicDBObject("v", expected.getValue()).append("e",
										expected.getExpirationDateAsString()).append("f",
										expected.getEffectiveDateAsString()))
																);
							
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
				
				List<Integer> modelLists = new ArrayList<Integer>();
				modelLists.add(48);
				List<Integer> modelLists2 = new ArrayList<Integer>();
				modelLists2.add(35);
				
				
				Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
			
				Map<String, Variable> variablesMap = new HashMap<String, Variable>();
				variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
			
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
			
				Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
				modelsContentMap.put(48, monthModelMap);
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting3", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting3");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
				double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
				newScoreListActual.add(score);
				}
				int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9934061356083235);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				Assert.assertEquals(1, value);
				
		}
	
	//This test is for variable S_DSL_APP_INT_ACC  which is not expired and newChangesVarValueMap does not contain it
	//i.e the variable is not coming from parsing bolt
	@Test
	public void executeScoringSingletonChangedMemberVarNotExpiredNotPresentInNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
				//Fake memberVariables collection
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", "SearsTesting4").append("2269", 1).append("2270",0.4));
				
						//fake changedMemberVariables Collection
						DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Change expected = new Change("2269", 12,
								simpleDateFormat.parse("2999-09-23"),
								simpleDateFormat.parse("2014-09-01"));
					
						changedMemberVar = db
								.getCollection("changedMemberVariables");
						String l_id = "SearsTesting4";
						changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
								"2269",
								new BasicDBObject("v", expected.getValue()).append("e",
										expected.getExpirationDateAsString()).append("f",
										expected.getEffectiveDateAsString()))
																);
							
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
				
				
				List<Integer> modelLists2 = new ArrayList<Integer>();
				modelLists2.add(35);
				modelLists2.add(48);
				
				Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
				//variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists2);
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists2);
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
			
				Map<String, Variable> variablesMap = new HashMap<String, Variable>();
				variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
				variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
				Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
				variablesMap2.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.15));
						
				
				Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
				monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap));
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap2));
				Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
				modelsContentMap.put(35, monthModelMap2);
				modelsContentMap.put(48, monthModelMap);
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting4", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting4");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
				double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
				newScoreListActual.add(score);
				}
				int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				int value2 = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getValue();
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9946749823142578);
				newScoreListExpected.add(0.9988874639671397);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				Assert.assertEquals(12,value);
				Assert.assertEquals(1,value2);
				Assert.assertEquals("2999-09-23", changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
				Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getExpirationDateAsString());
		}
	
	//This test is tested for variable S_DSL_APP_INT_ACC which is expired but newchangeVariableVaLueMap contains it
	@Test
	public void executeScoringSingletonChangedMemberVarExpiredPresentInNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting2").append("2269", 1).append("2270",0.4));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2014-08-23"),
						simpleDateFormat.parse("2014-09-01"));
						
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting2";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString()))
														);
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		
		
		Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
		Field variableModelsMapContent = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMapContent.setAccessible(true);
		variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
		modelsContentMap.put(48, monthModelMap);
		Field modelsMapContent = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMapContent.setAccessible(true);
		modelsMapContent.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		Field variableNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMapContents.setAccessible(true);
		variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		Field variableVidToNameMapContents = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMapContents.setAccessible(true);
		variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		Field variableNameToStrategyMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMapContents.setAccessible(true);
		variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting2", modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting2");
		Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
		double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
		newScoreListActual.add(score);
		}
	
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.9934061356083235);
		Assert.assertEquals(1, changedMemberVariablesMap.size());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
						Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
		Assert.assertEquals(newScoreListExpected, newScoreListActual);
		}
	
	//S_DSL_APP_INT_ACC is expired and S_DSL_APP_INT_ACC2 is not expired but both variables present in newChangesVariableValueMap
	//So, both variables value and date will get updated with newChangesVarValuesMap based on their strategy
	@Test
	public void executeScoringSingletonOneVarExpOneVarNotExpBothPresentinNewChangesVarValueMap() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting8").append("2269", 1).append("2270",0.4));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected2 = new Change("2270", 1,
						simpleDateFormat.parse("2014-08-23"),
						simpleDateFormat.parse("2014-09-01"));
				
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting8";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString()).append(
										"2270",
										new BasicDBObject("v", expected2.getValue()).append("e",
												expected2.getExpirationDateAsString()).append("f",
												expected2.getEffectiveDateAsString())))
														);
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
		List<Integer> modelLists = new ArrayList<Integer>();
		modelLists.add(48);
		List<Integer> modelLists2 = new ArrayList<Integer>();
		modelLists2.add(35);
		
		
		Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists2);
		Field variableModelsMapContent = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMapContent.setAccessible(true);
		variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
		Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
		variablesMap2.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
		monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap2));
		Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
		modelsContentMap.put(48, monthModelMap);
		modelsContentMap.put(35, monthModelMap2);
		Field modelsMapContent = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMapContent.setAccessible(true);
		modelsMapContent.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
		Field variableNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMapContents.setAccessible(true);
		variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
		Field variableVidToNameMapContents = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMapContents.setAccessible(true);
		variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
		Field variableNameToStrategyMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMapContents.setAccessible(true);
		variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting8", modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting8");
		Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
		double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
		newScoreListActual.add(score);
		}
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.9934391327651371);
		newScoreListExpected.add(0.9934061356083235);
		Assert.assertEquals(newScoreListExpected, newScoreListActual);
		Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
		Assert.assertEquals(0.401, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getExpirationDateAsString());
	}

		//S_DSL_APP_INT_ACC is expired and S_DSL_APP_INT_ACC2 is not expired and only S_DSL_APP_INT_ACC variable present in newChangesVariableValueMap
		//So, S_DSL_APP_INT_ACC value and date will get updated with newChangesVarValuesMap based on their strategy
		//S_DSL_APP_INT_ACC2 will be restored with changedMemVar dates and value
		//Both variables affect the modelId 35, so re-scoring will happen based on both variables
		@Test
		public void executeScoringSingletonOneVarExpOneVarNotExpButOnlyExpVarPresentinNewChangesVarValueMap() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
			//Fake memberVariables collection
			DB db = DBConnection.getDBConnection();
			DBCollection memVarColl = db.getCollection("memberVariables");
			memVarColl.insert(new BasicDBObject("l_id", "SearsTesting9").append("2269", 1).append("2270",0.4));
			
					//fake changedMemberVariables Collection
					DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
					Change expected = new Change("2269", 12,
							simpleDateFormat.parse("2999-09-23"),
							simpleDateFormat.parse("2014-09-01"));
					Change expected2 = new Change("2270", 1,
							simpleDateFormat.parse("2014-08-23"),
							simpleDateFormat.parse("2014-09-01"));
					
					changedMemberVar = db
							.getCollection("changedMemberVariables");
					String l_id = "SearsTesting9";
					changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
							"2269",
							new BasicDBObject("v", expected.getValue()).append("e",
									expected.getExpirationDateAsString()).append("f",
									expected.getEffectiveDateAsString()).append(
											"2270",
											new BasicDBObject("v", expected2.getValue()).append("e",
													expected2.getExpirationDateAsString()).append("f",
													expected2.getEffectiveDateAsString())))
															);
						
			Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
			newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
			List<Integer> modelLists = new ArrayList<Integer>();
			modelLists.add(48);
			List<Integer> modelLists2 = new ArrayList<Integer>();
			modelLists2.add(35);
			
			
			Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
			variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
			variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists2);
			Field variableModelsMapContent = ScoringSingleton.class
					.getDeclaredField("variableModelsMap");
			variableModelsMapContent.setAccessible(true);
			variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
			variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.015));
			Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
			monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
			modelsContentMap.put(35, monthModelMap2);
			Field modelsMapContent = ScoringSingleton.class
					.getDeclaredField("modelsMap");
			modelsMapContent.setAccessible(true);
			modelsMapContent.set(scoringSingletonObj,modelsContentMap);
			
			Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
			Field variableNameToVidMapContents = ScoringSingleton.class
					.getDeclaredField("variableNameToVidMap");
			variableNameToVidMapContents.setAccessible(true);
			variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
			
			Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
			variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
			variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
			Field variableVidToNameMapContents = ScoringSingleton.class
					.getDeclaredField("variableVidToNameMap");
			variableVidToNameMapContents.setAccessible(true);
			variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
			
			Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
			Field variableNameToStrategyMapContents = ScoringSingleton.class
					.getDeclaredField("variableNameToStrategyMap");
			variableNameToStrategyMapContents.setAccessible(true);
			variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
			
			Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting9", modelIdsList);
			Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting9");
			Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
			List<Double> newScoreListActual = new LinkedList<Double>();
			for(int modelId:modelIdsList){
			double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
			newScoreListActual.add(score);
			}
			List<Double> newScoreListExpected = new LinkedList<Double>();
			newScoreListExpected.add(0.9944863525392151);
			Assert.assertEquals(newScoreListExpected, newScoreListActual);
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getExpirationDateAsString());
			Assert.assertEquals("2999-09-23", changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
		}

		//S_DSL_APP_INT_ACC's expiration date is null, the chnagedMemVarMap will not be populated with that variable 
		//but S_DSL_APP_INT_ACC is present in newChangesVarValueMap, so S_DSL_APP_INT_ACC from newChangesVarValueMap will be populated into changedMemVarMap 
		@Test
		public void executeScoringSingletonNullExpiratioDateVarPresentInNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
				
			//Fake memberVariables collection
			DB db = DBConnection.getDBConnection();
			DBCollection memVarColl = db.getCollection("memberVariables");
			memVarColl.insert(new BasicDBObject("l_id", "SearsTesting10").append("2269", 1).append("2270",0.4));
			
					//fake changedMemberVariables Collection
					DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
											
					changedMemberVar = db
							.getCollection("changedMemberVariables");
					String l_id = "SearsTesting10";
			
			changedMemberVar.insert(new BasicDBObject("l_id",l_id).append("2269", new BasicDBObject("v", 12).append("e", null).append("f", null)));
						
			Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
			newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
			List<Integer> modelLists = new ArrayList<Integer>();
			modelLists.add(48);
			
			
			Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
			variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
			Field variableModelsMapContent = ScoringSingleton.class
					.getDeclaredField("variableModelsMap");
			variableModelsMapContent.setAccessible(true);
			variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
			modelsContentMap.put(48, monthModelMap);
			Field modelsMapContent = ScoringSingleton.class
					.getDeclaredField("modelsMap");
			modelsMapContent.setAccessible(true);
			modelsMapContent.set(scoringSingletonObj,modelsContentMap);
			
			Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
			Field variableNameToVidMapContents = ScoringSingleton.class
					.getDeclaredField("variableNameToVidMap");
			variableNameToVidMapContents.setAccessible(true);
			variableNameToVidMapContents.set(scoringSingletonObj,variableNameToVidMapContentsMap);
			
			Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
			variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
			Field variableVidToNameMapContents = ScoringSingleton.class
					.getDeclaredField("variableVidToNameMap");
			variableVidToNameMapContents.setAccessible(true);
			variableVidToNameMapContents.set(scoringSingletonObj, variableVidToNameMapContentsMap);
			
			Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
			Field variableNameToStrategyMapContents = ScoringSingleton.class
					.getDeclaredField("variableNameToStrategyMap");
			variableNameToStrategyMapContents.setAccessible(true);
			variableNameToStrategyMapContents.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
			
			Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap("SearsTesting10", modelIdsList);
			Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting10");
			Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
			List<Double> newScoreListActual = new LinkedList<Double>();
			for(int modelId:modelIdsList){
			double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
			newScoreListActual.add(score);
			}
		
			List<Double> newScoreListExpected = new LinkedList<Double>();
			newScoreListExpected.add(0.9934061356083235);
			Assert.assertEquals(1, changedMemberVariablesMap.size());
			Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
							Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
			Assert.assertEquals(newScoreListExpected, newScoreListActual);
			}
		
}
