package analytics.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import analytics.util.objects.Change;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

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
		scoringSingletonObj = constructor.newInstance();
		
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
		
		//regionalFactor data is cached, so populated before the execution of test cases
		DBCollection regionalFactorsCollection = db.getCollection("regionalAdjustmentFactors");
		regionalFactorsCollection.insert(new BasicDBObject("modelName", "model_regFactor").append("modelId", "100").append("state", "TN").append("factor", 0.2));
		scoringSingletonObj.populateModelsWithRegFactors();
		Set<String> modelIdsWithRegionalFactorsContents = new HashSet<String>();
		modelIdsWithRegionalFactorsContents.add("100");
		Field modelIdsWithRegionalFactors = ScoringSingleton.class
				.getDeclaredField("modelIdsWithRegionalFactors");
		modelIdsWithRegionalFactors.set(scoringSingletonObj,modelIdsWithRegionalFactorsContents);
	}

	@AfterClass
	public static void cleanUp(){
		if(db.toString().equalsIgnoreCase("FongoDB.test"))
			   db.dropDatabase();
			  else
			   Assert.fail("Something went wrong. Tests connected to " + db.toString());
		SystemPropertyUtility.dropDatabase();
	}
	
	//This integration test is check the re-scored value for modelIds 35 and 48 (a positive case)
	@Test
	public void executeScoringSingletonBasicPositiveCaseTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		String l_id = "SearsTesting";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));

		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2269", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("2270", 1,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
				"2269",
				new BasicDBObject("v", expected.getValue()).append("e",
						expected.getExpirationDateAsString()).append("f",
						expected.getEffectiveDateAsString())).append(
								"2270",
								new BasicDBObject("v", expected2.getValue()).append("e",
										expected2.getExpirationDateAsString()).append("f",
										expected2.getEffectiveDateAsString())));
					
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
		Field modelsMap = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
		Field variableVidToNameMap = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMap.setAccessible(true);
		variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
		Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
			double score = scoringSingletonObj.calcScore(memberVariablesMap, allChangesMap, modelId);
			newScoreListActual.add(score);
		}
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.99330748147035);
		newScoreListExpected.add(0.9934061356083235);
	//	expected:<[0.99330748147035, 0.9934061356083235]> but was:
	//			 <[0.9936314844931845, 0.9934061356083235]>

		Assert.assertEquals(newScoreListExpected, newScoreListActual);
		memVarColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		variableModelsMapContent.setAccessible(false);
		modelsMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		variableNameToStrategyMap.setAccessible(false);
	}

	//if all the existing changedMemberVariables are expired
	//previous value for the variable 2270 will be set from memberVariables (a double)
	// previous value for the Change returned by StrategySumSales strategy will be the sum of previous and current value for VID 2270
	@Test
	public void executeScoringSingletonAllChangedMemberVariablesExpiredTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
				String l_id = "SearsTesting2" ;
				//Fake memberVariables collection
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
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
				Field modelsMap = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMap.setAccessible(true);
				modelsMap.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
				Field variableNameToVidMap = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMap.setAccessible(true);
				variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
				Field variableVidToNameMap = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMap.setAccessible(true);
				variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
				Field variableNameToStrategyMap = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMap.setAccessible(true);
				variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
					double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
					newScoreListActual.add(score);
				}
			
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9933337551162632);
				newScoreListExpected.add(0.9934061356083235);
			//	<[0.9933337551162632, 0.9934061356083235]> but was:
			//	<[0.9933336888978759, 0.9934061356083235]>
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				
				memVarColl.remove(new BasicDBObject("l_id", l_id));
				changedMemberVar.remove(new BasicDBObject("l_id", l_id));
				variableModelsMapContent.setAccessible(false);
				modelsMap.setAccessible(false);
				variableNameToVidMap.setAccessible(false);
				variableNameToStrategyMap.setAccessible(false);
		}
	
		
	//if the incoming variable (in newchangeVarValueMap) is not expired for that member, allChanges will be populated with incoming variable
	// and the value will be set from strategy provided 
	@Test
	public void executeScoringSingletonchangedMemberVariablesNotExpiredPresentinNewChangesVariableTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{

				String l_id = "SearsTesting3";
				//Fake memberVariables collection
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
				
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
			
				changedMemberVar = SystemPropertyUtility.getDb()
						.getCollection("changedMemberVariables");
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
				Field modelsMap = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMap.setAccessible(true);
				modelsMap.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				Field variableNameToVidMap = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMap.setAccessible(true);
				variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				Field variableVidToNameMap = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMap.setAccessible(true);
				variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyCountTransactions");
				Field variableNameToStrategyMap = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMap.setAccessible(true);
				variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
					double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
					newScoreListActual.add(score);
				}
				int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				Assert.assertEquals(13, value);
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9944863525392151);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				
				memVarColl.remove(new BasicDBObject("l_id", l_id));
				changedMemberVar.remove(new BasicDBObject("l_id", l_id));
				variableModelsMapContent.setAccessible(false);
				modelsMap.setAccessible(false);
				variableNameToVidMap.setAccessible(false);
				variableNameToStrategyMap.setAccessible(false);
		}
	
	//This test is for variable S_DSL_APP_INT_ACC  which is not expired but not there in incoming var list from the feed (i.e. newChangesVarValueMap does not contain it)
	//S_DSL_APP_INT_ACC2 -- no strategy for this variable and the value will be from the changedMembervariable collection itself
	@Test
	public void executeScoringSingletonChangedMemberVarNotExpiredNotPresentInNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
				String l_id = "SearsTesting4";
				//Fake memberVariables collection
				DBCollection memVarColl = db.getCollection("memberVariables");
				memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString()))
														);
							
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
						
				List<Integer> modelLists = new ArrayList<Integer>();
				modelLists.add(35);
								
				Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
				variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists);
				Field variableModelsMap = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMap.setAccessible(true);
				variableModelsMap.set(scoringSingletonObj, variableModelsMapContentMap);
			
				Map<String, Variable> variablesMap = new HashMap<String, Variable>();
				variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
				variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
								
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap));
				Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
				modelsContentMap.put(35, monthModelMap);
				Field modelsMap = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMap.setAccessible(true);
				modelsMap.set(scoringSingletonObj,modelsContentMap);
				
				Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
				variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
				Field variableNameToVidMap = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMap.setAccessible(true);
				variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
				
				Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
				variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
				variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
				Field variableVidToNameMap = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMap.setAccessible(true);
				variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
				
				Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
				variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
				Field variableNameToStrategyMap = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMap.setAccessible(true);
				variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId:modelIdsList){
					double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
					newScoreListActual.add(score);
				}
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9946749823142578);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				
				memVarColl.remove(new BasicDBObject("l_id", l_id));
				changedMemberVar.remove(new BasicDBObject("l_id", l_id));
				variableModelsMap.setAccessible(false);
				modelsMap.setAccessible(false);
				variableNameToVidMap.setAccessible(false);
				variableNameToStrategyMap.setAccessible(false);
			}
	
	//This test is tested for variable incoming varaible S_DSL_APP_INT_ACC which is expired 
	@Test
	public void executeScoringSingletonChangedMemberVarExpiredPresentInNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		String l_id = "SearsTesting2";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = SystemPropertyUtility.getDb().getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2269", 12,
				simpleDateFormat.parse("2014-08-23"),
				simpleDateFormat.parse("2014-09-01"));
				
		changedMemberVar = db
				.getCollection("changedMemberVariables");
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
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, variableModelsMapContentMap);
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(48, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
		modelsContentMap.put(48, monthModelMap);
		Field modelsMap = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		Field variableVidToNameMap = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMap.setAccessible(true);
		variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
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
			
			memVarColl.remove(new BasicDBObject("l_id", l_id));
			changedMemberVar.remove(new BasicDBObject("l_id", l_id));
			variableModelsMap.setAccessible(false);
			modelsMap.setAccessible(false);
			variableNameToVidMap.setAccessible(false);
			variableNameToStrategyMap.setAccessible(false);
		}
	
	//S_DSL_APP_INT_ACC is expired and S_DSL_APP_INT_ACC2 is not expired but both variables present in newChangesVariableValueMap
	//So, both variables value and date will get updated with newChangesVarValuesMap based on their strategy
	@Test
	public void executeScoringSingletonOneVarExpOneVarNotExpBothPresentinNewChangesVarValueMap() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		String l_id = "SearsTesting8";
		//Fake memberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
		//fake changedMemberVariables Collection
		DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Change expected = new Change("2269", 12,
				simpleDateFormat.parse("2999-09-23"),
				simpleDateFormat.parse("2014-09-01"));
		Change expected2 = new Change("2270", 1,
				simpleDateFormat.parse("2014-08-23"),
				simpleDateFormat.parse("2014-09-01"));
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
		modelLists.add(35);
			
		Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
		variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists);
		Field variableModelsMap = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMap.setAccessible(true);
		variableModelsMap.set(scoringSingletonObj, variableModelsMapContentMap);
	
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
		variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
		Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
		monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
		Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
		modelsContentMap.put(35, monthModelMap);
		Field modelsMap = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMap.setAccessible(true);
		modelsMap.set(scoringSingletonObj,modelsContentMap);
		
		Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
		Field variableNameToVidMap = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMap.setAccessible(true);
		variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
		
		Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
		variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
		variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
		Field variableVidToNameMap = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMap.setAccessible(true);
		variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
		
		Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
		Field variableNameToStrategyMap = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMap.setAccessible(true);
		variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
		Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
			double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, modelId);
			newScoreListActual.add(score);
		}
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.9935361799759752);
		Assert.assertEquals(newScoreListExpected, newScoreListActual);
		Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
		Assert.assertEquals(0.401, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getValue());
		Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC2").getExpirationDateAsString());
		
		memVarColl.remove(new BasicDBObject("l_id", l_id));
		changedMemberVar.remove(new BasicDBObject("l_id", l_id));
		variableModelsMap.setAccessible(false);
		modelsMap.setAccessible(false);
		variableNameToVidMap.setAccessible(false);
		variableNameToStrategyMap.setAccessible(false);
	}

		//S_DSL_APP_INT_ACC2 is expired and S_DSL_APP_INT_ACC is not expired and only S_DSL_APP_INT_ACC2 variable present in newChangesVariableValueMap
		//So, S_DSL_APP_INT_ACC value and date will get updated with newChangesVarValuesMap based on their strategy
		//S_DSL_APP_INT_ACC2 will be restored with changedMemVar dates and value
		//Both variables affect the modelId 35, so re-scoring will happen based on both variables
		@Test
		public void executeScoringSingletonOneVarExpOneVarOtherNotExpButOnlyExpVarPresentinNewChangesVarValueMap() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			String l_id = "SearsTesting9";
			//Fake memberVariables collection
			DBCollection memVarColl = db.getCollection("memberVariables");
			memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));
			//fake changedMemberVariables Collection
			DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Change expected = new Change("2269", 12,
					simpleDateFormat.parse("2999-09-23"),
					simpleDateFormat.parse("2014-09-01"));
			Change expected2 = new Change("2270", 1,
					simpleDateFormat.parse("2014-08-23"),
					simpleDateFormat.parse("2014-09-01"));
		
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
			Field variableModelsMap = ScoringSingleton.class
					.getDeclaredField("variableModelsMap");
			variableModelsMap.setAccessible(true);
			variableModelsMap.set(scoringSingletonObj, variableModelsMapContentMap);
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
			variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.015));
			Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
			monthModelMap2.put(0, new Model(35, "Model_Name2", 0, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
			modelsContentMap.put(35, monthModelMap2);
			Field modelsMap = ScoringSingleton.class
					.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj,modelsContentMap);
			
			Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
			Field variableNameToVidMap = ScoringSingleton.class
					.getDeclaredField("variableNameToVidMap");
			variableNameToVidMap.setAccessible(true);
			variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
			
			Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
			variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
			variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
			Field variableVidToNameMap = ScoringSingleton.class
					.getDeclaredField("variableVidToNameMap");
			variableVidToNameMap.setAccessible(true);
			variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
			
			Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
			Field variableNameToStrategyMap = ScoringSingleton.class
					.getDeclaredField("variableNameToStrategyMap");
			variableNameToStrategyMap.setAccessible(true);
			variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
			
			Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
			Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
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
			
			memVarColl.remove(new BasicDBObject("l_id", l_id));
			changedMemberVar.remove(new BasicDBObject("l_id", l_id));
			variableModelsMap.setAccessible(false);
			modelsMap.setAccessible(false);
			variableNameToVidMap.setAccessible(false);
			variableNameToStrategyMap.setAccessible(false);
		}

		//This integration test is check the scored value with Regional factors for modelId 100(a positive case)
		@Test
		public void executeScoringSingletonBasicPositiveCaseWithRegionalFactorTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
			String l_id = "SearsTesting";
			//Fake memberVariables collection
			DBCollection memVarColl = db.getCollection("memberVariables");
			memVarColl.insert(new BasicDBObject("l_id", l_id).append("2269", 1).append("2270",0.4));

			//fake changedMemberVariables Collection
			DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Change expected = new Change("2269", 12,
					simpleDateFormat.parse("2999-09-23"),
					simpleDateFormat.parse("2014-09-01"));
			Change expected2 = new Change("2270", 1,
					simpleDateFormat.parse("2999-09-23"),
					simpleDateFormat.parse("2014-09-01"));
			changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
					"2269",
					new BasicDBObject("v", expected.getValue()).append("e",
							expected.getExpirationDateAsString()).append("f",
							expected.getEffectiveDateAsString())).append(
									"2270",
									new BasicDBObject("v", expected2.getValue()).append("e",
											expected2.getExpirationDateAsString()).append("f",
											expected2.getEffectiveDateAsString())));
			
			//fake memberInfo collection
			DBCollection memberInfoColl = db.getCollection("memberInfo");
			memberInfoColl.insert(new BasicDBObject("l_id", l_id).append("srs", "010").append("srs_zip", "00").append("kmt", "020").append("kmt_zip", "22").append("eid", "12334").append("eml_opt_in", "Y").append("st_cd", "TN"));
			
			String state = scoringSingletonObj.getMemberState(l_id);
								
			Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
			newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
			newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
			List<Integer> modelLists = new ArrayList<Integer>();
			modelLists.add(100);
						
			Map<String, List<Integer>> variableModelsMapContentMap = new HashMap<String, List<Integer>>();
			variableModelsMapContentMap.put("S_DSL_APP_INT_ACC", modelLists);
			variableModelsMapContentMap.put("S_DSL_APP_INT_ACC2", modelLists);
			Field variableModelsMapContent = ScoringSingleton.class
					.getDeclaredField("variableModelsMap");
			variableModelsMapContent.setAccessible(true);
			variableModelsMapContent.set(scoringSingletonObj, variableModelsMapContentMap);
		
			Map<String, Variable> variablesMap = new HashMap<String, Variable>();
			variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC",0.015));
			variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.05));
			Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
			monthModelMap.put(0, new Model(100, "Model_Name", 0, 5, variablesMap));
			Map<Integer, Map<Integer, Model>> modelsContentMap = new HashMap<Integer, Map<Integer, Model>>();
			modelsContentMap.put(100, monthModelMap);
			Field modelsMap = ScoringSingleton.class
					.getDeclaredField("modelsMap");
			modelsMap.setAccessible(true);
			modelsMap.set(scoringSingletonObj,modelsContentMap);
			
			Map<String, String> variableNameToVidMapContentsMap = new HashMap<String, String>() ;
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC", "2269");
			variableNameToVidMapContentsMap.put("S_DSL_APP_INT_ACC2", "2270");
			Field variableNameToVidMap = ScoringSingleton.class
					.getDeclaredField("variableNameToVidMap");
			variableNameToVidMap.setAccessible(true);
			variableNameToVidMap.set(scoringSingletonObj,variableNameToVidMapContentsMap);
			
			Map<String, String> variableVidToNameMapContentsMap = new HashMap<String, String>() ;
			variableVidToNameMapContentsMap.put("2269","S_DSL_APP_INT_ACC");
			variableVidToNameMapContentsMap.put("2270","S_DSL_APP_INT_ACC2");
			Field variableVidToNameMap = ScoringSingleton.class
					.getDeclaredField("variableVidToNameMap");
			variableVidToNameMap.setAccessible(true);
			variableVidToNameMap.set(scoringSingletonObj, variableVidToNameMapContentsMap);
			
			Map<String, String> variableNameToStrategyMapContentsMap = new HashMap<String, String>();
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
			variableNameToStrategyMapContentsMap.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
			Field variableNameToStrategyMap = ScoringSingleton.class
					.getDeclaredField("variableNameToStrategyMap");
			variableNameToStrategyMap.setAccessible(true);
			variableNameToStrategyMap.set(scoringSingletonObj, variableNameToStrategyMapContentsMap);
			
			Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			Map<String, Object> memberVariablesMap = scoringSingletonObj.createMemberVariableValueMap(l_id, modelIdsList);
			Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap(l_id);
			Map<String, Change> allChangesMap = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
			List<Double> newScoreListActual = new LinkedList<Double>();
			for(int modelId:modelIdsList){
				double score = scoringSingletonObj.calcScore(memberVariablesMap, allChangesMap, modelId);
				double regionalFactor= scoringSingletonObj.getRegionalFactor(modelId+"", state);
				double newScore = score*regionalFactor;
				newScoreListActual.add(newScore);
			}
			List<Double> newScoreListExpected = new LinkedList<Double>();
			newScoreListExpected.add(0.1986812926239022);
		
			Assert.assertEquals(newScoreListExpected, newScoreListActual);
			memVarColl.remove(new BasicDBObject("l_id", l_id));
			changedMemberVar.remove(new BasicDBObject("l_id", l_id));
			variableModelsMapContent.setAccessible(false);
			modelsMap.setAccessible(false);
			variableNameToVidMap.setAccessible(false);
			variableNameToStrategyMap.setAccessible(false);
		}

}
