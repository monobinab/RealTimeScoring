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

	Map<String, List<Integer>> variableModelsMapContents = null;
	Map<Integer, Map<Integer, Model>> modelsMapContent= null;
	Map<String, String> variableNameToVidMapContents = null;
	Map<String, String> variableVidToNameMapContents = null;
	Map<String, String> variableNameToStrategyMapContents = null;

	public Map<String, String> getVariableNameToStrategyMapContents() {
		return variableNameToStrategyMapContents;
	}

	public void setVariableNameToStrategyMapContents(
			Map<String, String> variableNameToStrategyMapContents) {
		this.variableNameToStrategyMapContents = variableNameToStrategyMapContents;
	}

	public Map<String, String> getVariableVidToNameMapContents() {
		return variableVidToNameMapContents;
	}

	public void setVariableVidToNameMapContents(
			Map<String, String> variableVidToNameMapContents) {
		this.variableVidToNameMapContents = variableVidToNameMapContents;
	}

	public Map<String, String> getVariableNameToVidMapContents() {
		return variableNameToVidMapContents;
	}

	public void setVariableNameToVidMapContents(
			Map<String, String> variableNameToVidMapContents) {
		this.variableNameToVidMapContents = variableNameToVidMapContents;
	}

	public Map<Integer, Map<Integer, Model>> getModelsMapContent() {
		return modelsMapContent;
	}

	public void setModelsMapContent(
			Map<Integer, Map<Integer, Model>> modelsMapContent) {
		this.modelsMapContent = modelsMapContent;
	}

	public Map<String, List<Integer>> getVariableModelsMapContents() {
		return variableModelsMapContents;
	}

	public void setVariableModelsMapContents(
			Map<String, List<Integer>> variableModelsMapContents) {
		this.variableModelsMapContents = variableModelsMapContents;
	}

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
		
	
				//Fake modelVariables collection
				/*DBCollection modelVariablesColl = db.getCollection("modelVariables");
				
				String modelVar = "{'modelId':46,'modelDescription':'All cooking Appliances','constant':7,'modelName':'model_Name2','month':0,'variable':[{'name':'S_HOME_ALL','coefficient':0.075},{'name':'BOOST_S_DSL_APP_INT_ACC','coefficient':0.75},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject1 = (DBObject) JSON.parse(modelVar);
				String modelVar2 = "{'modelId':35,'modelDescription':'All Appliances','constant':5,'modelName':'model_Name','month':10,'variable':[{'name':'S_DSL_APP_INT_ACC','coefficient':0.002},{'name':'S_HOME_6M_IND','coefficient':0.0015},{'name':'S_HOME_6M_IND_ALL','coefficient':0.0915},{'name':'S_DSL_APP_INT_ACC2','coefficient':0.0915},{'name':'S_HOME_6M_IND','coefficient':0.0015},{'name':'S_HOME_6M_IND_ALL','coefficient':0.0915},{'name':'S_DSL_APP_INT_ACC2','coefficient':0.0915},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.0915}]}";
				DBObject dbObject2 = (DBObject) JSON.parse(modelVar2);
				String modelVar3 = "{'modelId':30,'modelDescription':'Tools','constant':5,'modelName':'model_Name3','month':11,'variable':[{'name':'S_HOME_ALL','coefficient':0.075},{'name':'BOOST_S_DSL_APP_INT_ACC','coefficient':0.75},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject3 = (DBObject) JSON.parse(modelVar3);
				String modelVar4 = "{'modelId':51,'modelDescription':'Tools','constant':3,'modelName':'model_Name4','month':10,'variable':[{'name':'S_HOME_ALL','coefficient':0.075},{'name':'BOOST_S_DSL_APP_INT_ACC','coefficient':0.75},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject4 = (DBObject) JSON.parse(modelVar4);
				String modelVar5 = "{'modelId':15,'modelDescription':'All Tools','constant':3,'modelName':'model_Name7','month':10,'variable':[{'name':'S_DSL_APP_INT_ACC2','coefficient':0.75},{'name':'S_DSL_APP_INT_ACC_FTWR_TRS','coefficient':0.075},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject5 = (DBObject) JSON.parse(modelVar5);
				String modelVar6 = "{'modelId':32,'modelDescription':'Apparel','constant':3,'modelName':'model_Name5','month':10,'variable':[{'name':'S_HOME_ALL','coefficient':0.075},{'name':'BOOST_S_DSL_APP_INT_ACC','coefficient':0.75},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject6 = (DBObject) JSON.parse(modelVar6);
				String modelVar7 = "{'modelId':04,'modelDescription':'Shoes','constant':3,'modelName':'model_Name6','month':10,'variable':[{'name':'S_DSL_APP_INT_ACC2','coefficient':0.75},{'name':'S_DSL_APP_INT_ACC_FTWR_TRS','coefficient':0.075},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject7 = (DBObject) JSON.parse(modelVar7);
				String modelVar8 = "{'modelId':48,'modelDescription':'Kids Apparel','constant':3,'modelName':'model_Name10','month':0,'variable':[{'name':'S_DSL_APP_INT_ACC','coefficient':0.002},{'name':'S_HOME_6M_IND','coefficient':0.0015},{'name':'S_HOME_6M_IND_ALL','coefficient':0.0915},{'name':'S_DSL_APP_INT_ACC2','coefficient':0.0915},{'name':'S_HOME_6M_IND','coefficient':0.0015},{'name':'S_HOME_6M_IND_ALL','coefficient':0.0915},{'name':'S_DSL_APP_INT_ACC2','coefficient':0.0915},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.0915}]}";
				DBObject dbObject8 = (DBObject) JSON.parse(modelVar8);
				String modelVar9 = "{'modelId':25,'modelDescription':'Mens Apparel','constant':3,'modelName':'model_Name8','month':10,'variable':[{'name':'S_DSL_APP_INT_ACC2','coefficient':0.75},{'name':'S_DSL_APP_INT_ACC_FTWR_TRS','coefficient':0.075},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject9 = (DBObject) JSON.parse(modelVar9);
				String modelVar10 = "{'modelId':60,'modelDescription':'Womens Apparel','constant':3,'modelName':'model_Name9','month':10,'variable':[{'name':'S_DSL_APP_INT_ACC2','coefficient':0.75},{'name':'S_DSL_APP_INT_ACC_FTWR_TRS','coefficient':0.075},{'name':'S_DSL_APP_INT_BOTH','coefficient':0.175}]}";
				DBObject dbObject10 = (DBObject) JSON.parse(modelVar10);
				
				modelVariablesColl.insert(dbObject1);
				modelVariablesColl.insert(dbObject2);
				modelVariablesColl.insert(dbObject3);
				modelVariablesColl.insert(dbObject4);
				modelVariablesColl.insert(dbObject5);
				modelVariablesColl.insert(dbObject6);
				modelVariablesColl.insert(dbObject7);
				modelVariablesColl.insert(dbObject8);
				modelVariablesColl.insert(dbObject9);
				modelVariablesColl.insert(dbObject10);
				
				//Fake variables collection
				DBCollection variablesColl = db.getCollection("Variables");
				DBObject varObj = new BasicDBObject("name", "S_HOME_6M_IND").append("VID", 2268).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOffFlag");
				DBObject varObj2 = new BasicDBObject("name", "S_DSL_APP_INT_ACC").append("VID", 2269).append("trs_lvl_fl", 0).append("strategy", "StrategyDaysSinceLast");
				DBObject varObj3 = new BasicDBObject("name", "S_DSL_APP_INT_ACC2").append("VID", 2270).append("trs_lvl_fl", 0).append("strategy", "StrategySumSales");
				DBObject varObj4 = new BasicDBObject("name", "S_HOME_6M_IND_ALL").append("VID", 2271).append("trs_lvl_fl", 0).append("strategy", "StrategyDaysSinceLast");
				DBObject varObj5 = new BasicDBObject("name", "S_DSL_APP_INT_BOTH").append("VID", 2272).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOffFlag");
				DBObject varObj6 = new BasicDBObject("name", "S_HOME_ALL").append("VID", 2276).append("trs_lvl_fl", 0).append("strategy", "StrategyCountTraits");
				DBObject varObj7 = new BasicDBObject("name", "S_DSL_APP_INT_ACC_FTWR_TRS").append("VID", 2273).append("trs_lvl_fl", 0).append("strategy", "StrategyCountTransactions");
				DBObject varObj9 = new BasicDBObject("name", "S_DSL_APP_INT_ACC_FTWR").append("VID", 2277).append("trs_lvl_fl", 0).append("strategy", "StrategyCountTraits");
				DBObject varObj10 = new BasicDBObject("name", "S_DSL_APP_INT_ACC_FTWR_ALL").append("VID", 2274).append("trs_lvl_fl", 0).append("strategy", "StrategyDaysSinceLast");
				DBObject varObj11 = new BasicDBObject("name", "S_DSL_APP_INT_ACC_FTWR_MEM").append("VID", 2275).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOffFlag");
				DBObject varObj12 = new BasicDBObject("name", "BOOST_S_DSL_APP_INT_ACC").append("VID", 2281).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOffFlag");
				DBObject varObj13 = new BasicDBObject("name", "BOOST_S_DSL_APP_INT_ACC2").append("VID", 2282).append("trs_lvl_fl", 0).append("strategy", "StrategyDaysSinceLast");
				DBObject varObj14 = new BasicDBObject("name", "BOOST_SYW_WANT_TOYS_TCOUNT").append("VID", 2283).append("trs_lvl_fl", 0).append("strategy", "StrategySumSales");
				DBObject varObj15 = new BasicDBObject("name", "BOOST_SYW_WANT_TOYS_TCOUNT2").append("VID", 2284).append("trs_lvl_fl", 0).append("strategy", "StrategyTurnOffFlag");
				variablesColl.insert(varObj);
				variablesColl.insert(varObj2);
				variablesColl.insert(varObj3);
				variablesColl.insert(varObj4);
				variablesColl.insert(varObj5);
				variablesColl.insert(varObj6);
				variablesColl.insert(varObj7);
				variablesColl.insert(varObj9);
				variablesColl.insert(varObj10);
				variablesColl.insert(varObj11);
				variablesColl.insert(varObj12);
				variablesColl.insert(varObj13);
				variablesColl.insert(varObj14);
				variablesColl.insert(varObj15);
				
				DBObject dbj = variablesColl.findOne(new BasicDBObject("name", "BOOST_SYW_WANT_TOYS_TCOUNT2"));
				System.out.println(dbj);*/
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
				List<Integer> modelLists4 = new ArrayList<Integer>();
				modelLists4.add(32);
				modelLists4.add(04);
				List<Integer> modelLists5 = new ArrayList<Integer>();
				modelLists5.add(35);
				modelLists5.add(15);
				modelLists5.add(25);
				modelLists5.add(60);
				
				variableModelsMapContents = new HashMap<String, List<Integer>>();
				variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", modelLists);
				variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR", modelLists2);
				variableModelsMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT", modelLists2);
				variableModelsMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT2", modelLists3);
				variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", modelLists4);
				variableModelsMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", modelLists);
				variableModelsMapContents.put("S_HOME_6M_IND", modelLists5);
				variableModelsMapContents.put("S_DSL_APP_INT_ACC", modelLists);
				variableModelsMapContents.put("S_DSL_APP_INT_ACC2", modelLists4);
				variableModelsMapContents.put("S_HOME_6M_IND_ALL", modelLists);
				variableModelsMapContents.put("S_DSL_APP_INT_BOTH", modelLists);
				variableModelsMapContents.put("S_HOME_ALL", modelLists);
				variableModelsMapContents.put("BOOST_S_DSL_APP_INT_ACC", modelLists);
				variableModelsMapContents.put("BOOST_S_DSL_APP_INT_ACC2", modelLists);
				setVariableModelsMapContents(variableModelsMapContents);
			
				//modelsMap
						Map<String, Variable> variablesMap = new HashMap<String, Variable>();
						variablesMap.put("S_DSL_APP_INT_ACC", new Variable("S_DSL_APP_INT_ACC", 0.002));
						variablesMap.put("S_HOME_6M_IND", new Variable("S_HOME_6M_IND", 0.0015));
						variablesMap.put("S_HOME_6M_IND_ALL", new Variable("S_HOME_6M_IND_ALL",0.0915));
						variablesMap.put("S_DSL_APP_INT_ACC2", new Variable("S_DSL_APP_INT_ACC2",0.0915));
						variablesMap.put("S_DSL_APP_INT_BOTH", new Variable("S_DSL_APP_INT_BOTH",0.0915));
						Map<String, Variable> variablesMap2 = new HashMap<String, Variable>();
						variablesMap2.put("S_HOME_ALL", new Variable("S_HOME_ALL", 0.075));
						variablesMap2.put("BOOST_S_DSL_APP_INT_ACC", new Variable("S_HOME_ALL", 0.75));
						variablesMap2.put("S_DSL_APP_INT_BOTH", new Variable("S_HOME_ALL", 0.175));
						Map<String, Variable> variablesMap3 = new HashMap<String, Variable>();
						variablesMap3.put("S_HOME_ALL2", new Variable("S_HOME_ALL2", 0.075));
						variablesMap3.put("S_HOME_ALL3", new Variable("S_HOME_ALL3", 0.75));
						variablesMap3.put("S_HOME_ALL4", new Variable("S_HOME_ALL4", 0.175));
						
						Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
						monthModelMap.put(0, new Model(35, "Model_Name", 0, 5, variablesMap));
						Map<Integer, Model> monthModelMap4 = new HashMap<Integer, Model>();
						monthModelMap4.put(0, new Model(46, "Model_Name2", 0, 7, variablesMap2));
						Map<Integer, Model> monthModelMap5 = new HashMap<Integer, Model>();
						monthModelMap5.put(0, new Model(30, "Model_Name3", 0, 7, variablesMap));
						Map<Integer, Model> monthModelMap2 = new HashMap<Integer, Model>();
						monthModelMap2.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(51, "Model_Name4", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap));
						Map<Integer, Model> monthModelMap3 = new HashMap<Integer, Model>();
						monthModelMap3.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(32, "Model_Name5", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap2));
						Map<Integer, Model> monthModelMap6 = new HashMap<Integer, Model>();
						monthModelMap6.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(04, "Model_Name6", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap3));
						Map<Integer, Model> monthModelMap7 = new HashMap<Integer, Model>();
						monthModelMap7.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(15, "Model_Name7", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap3));
						Map<Integer, Model> monthModelMap8 = new HashMap<Integer, Model>();
						monthModelMap8.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(25, "Model_Name8", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap3));
						Map<Integer, Model> monthModelMap9 = new HashMap<Integer, Model>();
						monthModelMap9.put(Calendar.getInstance().get(Calendar.MONTH) + 1, new Model(60, "Model_Name9", Calendar.getInstance().get(Calendar.MONTH) + 1, 3,variablesMap3));
						Map<Integer, Model> monthModelMap10 = new HashMap<Integer, Model>();
						monthModelMap10.put(0, new Model(48, "Model_Name10", 12, 3,variablesMap));
								
						modelsMapContent = new HashMap<Integer, Map<Integer, Model>>();
						modelsMapContent.put(35, monthModelMap);
						modelsMapContent.put(46, monthModelMap4);
						modelsMapContent.put(30, monthModelMap5);
						modelsMapContent.put(51, monthModelMap2);
						modelsMapContent.put(32, monthModelMap3);
						modelsMapContent.put(04, monthModelMap6);
						modelsMapContent.put(15, monthModelMap7);
						modelsMapContent.put(25, monthModelMap8);
						modelsMapContent.put(60, monthModelMap9);
						modelsMapContent.put(48, monthModelMap10);
						setModelsMapContent(modelsMapContent);
		
		variableNameToVidMapContents = new HashMap<String, String>();
		variableNameToVidMapContents.put("S_HOME_6M_IND", "2268");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC", "2269");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC2", "2270"); 
		variableNameToVidMapContents.put("S_HOME_6M_IND_ALL", "2271"); 
		variableNameToVidMapContents.put("S_DSL_APP_INT_BOTH", "2272");
		variableNameToVidMapContents.put("S_HOME_ALL", "2276");
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", "2273"); 
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR", "2277"); 
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", "2274"); 
		variableNameToVidMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", "2275"); 
		variableNameToVidMapContents.put("BOOST_S_DSL_APP_INT_ACC", "2281");
		variableNameToVidMapContents.put("BOOST_S_DSL_APP_INT_ACC2", "2282");
		variableNameToVidMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT", "2283");
		variableNameToVidMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT2", "2284");
		variableNameToVidMapContents.put("S_HOME_ALL2", "2290");
		variableNameToVidMapContents.put("S_HOME_ALL3", "2291");
		variableNameToVidMapContents.put("S_HOME_ALL4", "2292");
		setVariableNameToVidMapContents(variableNameToVidMapContents);
				
		variableVidToNameMapContents = new HashMap<String, String>();
		variableVidToNameMapContents.put("2268","S_HOME_6M_IND");
		variableVidToNameMapContents.put("2269","S_DSL_APP_INT_ACC");
		variableVidToNameMapContents.put("2270","S_DSL_APP_INT_ACC2"); 
		variableVidToNameMapContents.put("2271","S_HOME_6M_IND_ALL"); 
		variableVidToNameMapContents.put( "2272","S_DSL_APP_INT_BOTH");
		variableVidToNameMapContents.put( "2276","S_HOME_ALL");
		variableVidToNameMapContents.put("2273","S_DSL_APP_INT_ACC_FTWR_TRS"); 
		variableVidToNameMapContents.put( "2277","S_DSL_APP_INT_ACC_FTWR"); 
		variableVidToNameMapContents.put( "2274","S_DSL_APP_INT_ACC_FTWR_ALL"); 
		variableVidToNameMapContents.put( "2275","S_DSL_APP_INT_ACC_FTWR_MEM"); 
		variableVidToNameMapContents.put("2281","BOOST_S_DSL_APP_INT_ACC");
		variableVidToNameMapContents.put( "2282","BOOST_S_DSL_APP_INT_ACC2");
		variableVidToNameMapContents.put( "2283","BOOST_SYW_WANT_TOYS_TCOUNT");
		variableVidToNameMapContents.put( "2284","BOOST_SYW_WANT_TOYS_TCOUNT2");
		variableVidToNameMapContents.put( "2290","S_HOME_ALL2");
		variableVidToNameMapContents.put( "2291","S_HOME_ALL3");
		variableVidToNameMapContents.put( "2292","S_HOME_ALL4");
		setVariableVidToNameMapContents(variableVidToNameMapContents);
		
		variableNameToStrategyMapContents = new HashMap<String, String>();
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC2", "StrategySumSales");
		variableNameToStrategyMapContents.put("S_HOME_6M_IND_ALL", "StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_TRS", "StrategyCountTransactions");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_ALL", "StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR_MEM", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_HOME_6M_IND", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC", "StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_BOTH", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_HOME_ALL", "StrategyCountTraits");
		variableNameToStrategyMapContents.put("S_DSL_APP_INT_ACC_FTWR", "StrategyCountTraits");
		variableNameToStrategyMapContents.put("BOOST_S_DSL_APP_INT_ACC", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("BOOST_S_DSL_APP_INT_ACC2", "StrategyDaysSinceLast");
		variableNameToStrategyMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT", "StrategySumSales");
		variableNameToStrategyMapContents.put("BOOST_SYW_WANT_TOYS_TCOUNT2", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_HOME_ALL2", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_HOME_ALL3", "StrategyTurnOffFlag");
		variableNameToStrategyMapContents.put("S_HOME_ALL4", "StrategyTurnOffFlag");
		setVariableNameToStrategyMapContents(variableNameToStrategyMapContents);
	}
	
	@After
	public void tearDown() throws Exception {
	}

	//This integration test is check the re-scored value for modelIds 35 and 48 (a positive case)
	@Test
	public void executeScoringSingletonTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting").append("2268", 1).append("2269",0.4).append("2270", 0.06).append("2273", 0.04));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected2 = new Change("2271", 12,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected3 = new Change("2275", 12,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected4 = new Change("2273", 1,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString())).append(
										"2271",
										new BasicDBObject("v", expected2.getValue()).append("e",
												expected2.getExpirationDateAsString()).append("f",
												expected2.getEffectiveDateAsString())).append(
														"2275",
														new BasicDBObject("v", expected3.getValue()).append("e",
																expected3.getExpirationDateAsString()).append("f",
																expected3.getEffectiveDateAsString())).append(
																		"2273",
																		new BasicDBObject("v", expected4.getValue()).append("e",
																				expected4.getExpirationDateAsString()).append("f",
																				expected4.getEffectiveDateAsString())));
					
		Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
		newChangesVarValueMap.put("S_HOME_6M_IND_ALL", "1");
		newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_MEM", "1.0");
				
		Field variableModelsMapContent = ScoringSingleton.class
				.getDeclaredField("variableModelsMap");
		variableModelsMapContent.setAccessible(true);
		variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
		
		Field modelsMapContent = ScoringSingleton.class
				.getDeclaredField("modelsMap");
		modelsMapContent.setAccessible(true);
		modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
		
		Field variableNameToVidMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToVidMap");
		variableNameToVidMapContents.setAccessible(true);
		variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
		
		Field variableVidToNameMapContents = ScoringSingleton.class
				.getDeclaredField("variableVidToNameMap");
		variableVidToNameMapContents.setAccessible(true);
		variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
		
		Field variableNameToStrategyMapContents = ScoringSingleton.class
				.getDeclaredField("variableNameToStrategyMap");
		variableNameToStrategyMapContents.setAccessible(true);
		variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
		
		Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
		Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting", modelIdsList);
		Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting");
		Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
		List<Double> newScoreListActual = new LinkedList<Double>();
		for(int modelId:modelIdsList){
		double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, 35);
		newScoreListActual.add(score);
		}
		List<Double> newScoreListExpected = new LinkedList<Double>();
		newScoreListExpected.add(0.993943149103568);
		newScoreListExpected.add(0.993943149103568);
		Assert.assertEquals(newScoreListExpected, newScoreListActual);
	}

	//This test is check, if all the variables in changedMemberVariables are expired
	//newChangesVarValueMap, from parsing bolt which needs re-scoring is populated into changedMemberVariables map
	@Test
	public void executeScoringSingletonExpDateTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
		
		//Fake memberVariables collection
		DB db = DBConnection.getDBConnection();
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting2").append("2268", 1).append("2269",0.4).append("2270", 0.06).append("2273", 0.04));
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2013-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected2 = new Change("2071", 12,
						simpleDateFormat.parse("2011-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected3 = new Change("2275", 12,
						simpleDateFormat.parse("2012-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected4 = new Change("2273", 12,
						simpleDateFormat.parse("2012-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting2";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString())).append(
										"2271",
										new BasicDBObject("v", expected2.getValue()).append("e",
												expected2.getExpirationDateAsString()).append("f",
												expected2.getEffectiveDateAsString())).append(
														"2275",
														new BasicDBObject("v", expected3.getValue()).append("e",
																expected3.getExpirationDateAsString()).append("f",
																expected3.getEffectiveDateAsString())).append(
																		"2273",
																		new BasicDBObject("v", expected4.getValue()).append("e",
																				expected4.getExpirationDateAsString()).append("f",
																				expected4.getEffectiveDateAsString())));
						
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
				newChangesVarValueMap.put("S_HOME_6M_IND_ALL", "1");
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC_FTWR_MEM", "1.0");
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
				
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
				
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
				
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
				
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting2", modelIdsList);
				System.out.println(memberVariablesMap);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting2");
				//System.out.println(changedMemberVariablesMap.size());
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				System.out.println(changedMemVariablesStrategy.get("S_HOME_6M_IND_ALL").getValue());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC_FTWR_MEM").getValue());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
				double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, 35);
				//System.out.println(score);
				/*System.out.println(changedMemberVariablesMap.size());
				System.out.println(changedMemVariablesStrategy.size());
				System.out.println(changedMemVariablesStrategy.keySet());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC_FTWR_MEM").getExpirationDateAsString());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getEffectiveDateAsString());
				System.out.println(changedMemVariablesStrategy.get("S_HOME_6M_IND_ALL").getEffectiveDateAsString());
				double score = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy, 35);
				System.out.println(changedMemVariablesStrategy.get("S_HOME_6M_IND_ALL").getValue());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
				System.out.println(changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC_FTWR_MEM").getValue());*/
				
				Assert.assertEquals(3, changedMemVariablesStrategy.size());
				Assert.assertEquals(simpleDateFormat.format(new LocalDate(new Date()).plusDays(2).toDateMidnight().toDate()), changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getExpirationDateAsString());
				Assert.assertEquals(0, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC_FTWR_MEM").getValue());
				Assert.assertEquals(1, changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue());
				Assert.assertEquals(1, changedMemVariablesStrategy.get("S_HOME_6M_IND_ALL").getValue());
				Assert.assertEquals(0.993943149103568, score);
		}
	
	//This test is checked for null newChangesVarValueMap from ParsingBolt. 
	//no re-scoring will happen
	@Test
	public void executeScoringSingletonNewChangesVarNullCheckTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
		//Fake memeberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting2").append("2290", 1).append("2291",0.4).append("2292", 0.06));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2999-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected2 = new Change("2071", 12,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected3 = new Change("2275", 12,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				Change expected4 = new Change("2273", 12,
						simpleDateFormat.parse("2999-10-23"),
						simpleDateFormat.parse("2014-09-01"));
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting2";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString())).append(
										"2271",
										new BasicDBObject("v", expected2.getValue()).append("e",
												expected2.getExpirationDateAsString()).append("f",
												expected2.getEffectiveDateAsString())).append(
														"2275",
														new BasicDBObject("v", expected3.getValue()).append("e",
																expected3.getExpirationDateAsString()).append("f",
																expected3.getEffectiveDateAsString())).append(
																		"2273",
																		new BasicDBObject("v", expected4.getValue()).append("e",
																				expected4.getExpirationDateAsString()).append("f",
																				expected4.getEffectiveDateAsString())));
						
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
				
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
				
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
				
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
				
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting2", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting2");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				for(int modelId: modelIdsList){
				 scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy,modelId);
				}
			
		}
	
	
	//This test case is for variable S_DSL_APP_INT_ACC which is not expired in changedMemberVaribles and newchangeVarValueMap also contains it
	//The value for that var will be set from executestrategy method 
	@Test
	public void executeScoringSingletonNewChangesVarTest() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
		//Fake memeberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting4").append("2269", 1).append("2270",0.4).append("2292", 0.06));
		
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
								expected.getEffectiveDateAsString())));
						
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
				
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
				
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
				
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
				
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting4", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting4");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				int value =  (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId: modelIdsList){
				double newScore = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy,modelId);
				newScoreListActual.add(newScore);
			}
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.993558938316889);
				newScoreListExpected.add(0.9542877964053181);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				Assert.assertEquals(1, value);
				
		}
	
	//This test is for variable S_DSL_APP_INT_ACC  which is not expired and newChangesVarValueMap does not contain it
	//i.e the variable is not from parsing bolt
	@Test
	public void executeScoringSingletonNewChangesVarTest2() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
		//Fake memeberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting4").append("2269", 1).append("2270",0.4).append("2292", 0.06));
		
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
								expected.getEffectiveDateAsString())));
						
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
				
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
				
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
				
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
				
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting4", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting4");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId: modelIdsList){
				double newScore = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy,modelId);
				newScoreListActual.add(newScore);
			}
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.9977621514787237);
				newScoreListExpected.add(0.9836975006285591);
			//	Assert.assertEquals(newScoreListExpected, newScoreListActual);
				Assert.assertEquals(12,value);
		}
	
	//This test is tested for variable S_DSL_APP_INT_ACC which is expired but newchangeVariableVaLueMap contains it
	@Test
	public void executeScoringSingletonNewChangesVarTest3() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
			
		DB db = DBConnection.getDBConnection();
		
		//Fake memeberVariables collection
		DBCollection memVarColl = db.getCollection("memberVariables");
		memVarColl.insert(new BasicDBObject("l_id", "SearsTesting5").append("2269", 1).append("2270",0.4).append("2292", 0.06));
		
				//fake changedMemberVariables Collection
				DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
				Change expected = new Change("2269", 12,
						simpleDateFormat.parse("2012-09-23"),
						simpleDateFormat.parse("2014-09-01"));
				
				changedMemberVar = db
						.getCollection("changedMemberVariables");
				String l_id = "SearsTesting5";
				changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
						"2269",
						new BasicDBObject("v", expected.getValue()).append("e",
								expected.getExpirationDateAsString()).append("f",
								expected.getEffectiveDateAsString())));
						
				Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
				newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
				
				Field variableModelsMapContent = ScoringSingleton.class
						.getDeclaredField("variableModelsMap");
				variableModelsMapContent.setAccessible(true);
				variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
				
				Field modelsMapContent = ScoringSingleton.class
						.getDeclaredField("modelsMap");
				modelsMapContent.setAccessible(true);
				modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
				
				Field variableNameToVidMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToVidMap");
				variableNameToVidMapContents.setAccessible(true);
				variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
				
				Field variableVidToNameMapContents = ScoringSingleton.class
						.getDeclaredField("variableVidToNameMap");
				variableVidToNameMapContents.setAccessible(true);
				variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
				
				Field variableNameToStrategyMapContents = ScoringSingleton.class
						.getDeclaredField("variableNameToStrategyMap");
				variableNameToStrategyMapContents.setAccessible(true);
				variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
				
				Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
			
				Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting5", modelIdsList);
				Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting5");
				Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
				int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
				List<Double> newScoreListActual = new LinkedList<Double>();
				for(int modelId: modelIdsList){
				double newScore = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy,modelId);
				newScoreListActual.add(newScore);
			}
				List<Double> newScoreListExpected = new LinkedList<Double>();
				newScoreListExpected.add(0.993558938316889);
				newScoreListExpected.add(0.9542877964053181);
				Assert.assertEquals(newScoreListExpected, newScoreListActual);
				Assert.assertEquals(1, value);
		}
	
	//This test is tested for variable S_DSL_APP_INT_ACC which is expired but newchangeVariableVaLueMap contains it
		@Test
		public void executeScoringSingletonNewChangesVarTest4() throws SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException, RealTimeScoringException, ConfigurationException, ParseException{
				
			DB db = DBConnection.getDBConnection();
			
			//Fake memeberVariables collection
			DBCollection memVarColl = db.getCollection("memberVariables");
			memVarColl.insert(new BasicDBObject("l_id", "SearsTesting5").append("2269", 1).append("2270",0.4).append("2292", 0.06));
			
					//fake changedMemberVariables Collection
					DBCollection changedMemberVar = db.getCollection("changedMemberVariables");
					SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
					Change expected = new Change("2269", 12,
							simpleDateFormat.parse("2012-09-23"),
							simpleDateFormat.parse("2014-09-01"));
					
					changedMemberVar = db
							.getCollection("changedMemberVariables");
					String l_id = "SearsTesting5";
					changedMemberVar.insert(new BasicDBObject("l_id", l_id).append(
							"2269",
							new BasicDBObject("v", expected.getValue()).append("e",
									expected.getExpirationDateAsString()).append("f",
									expected.getEffectiveDateAsString())));
							
					Map<String, String> newChangesVarValueMap = new HashMap<String, String>();
					newChangesVarValueMap.put("S_DSL_APP_INT_ACC", "0.001");
					newChangesVarValueMap.put("S_DSL_APP_INT_ACC2", "0.001");
					
					Field variableModelsMapContent = ScoringSingleton.class
							.getDeclaredField("variableModelsMap");
					variableModelsMapContent.setAccessible(true);
					variableModelsMapContent.set(scoringSingletonObj, getVariableModelsMapContents());
					
					Field modelsMapContent = ScoringSingleton.class
							.getDeclaredField("modelsMap");
					modelsMapContent.setAccessible(true);
					modelsMapContent.set(scoringSingletonObj, getModelsMapContent());
					
					Field variableNameToVidMapContents = ScoringSingleton.class
							.getDeclaredField("variableNameToVidMap");
					variableNameToVidMapContents.setAccessible(true);
					variableNameToVidMapContents.set(scoringSingletonObj, getVariableNameToVidMapContents());
					
					Field variableVidToNameMapContents = ScoringSingleton.class
							.getDeclaredField("variableVidToNameMap");
					variableVidToNameMapContents.setAccessible(true);
					variableVidToNameMapContents.set(scoringSingletonObj, getVariableVidToNameMapContents());
					
					Field variableNameToStrategyMapContents = ScoringSingleton.class
							.getDeclaredField("variableNameToStrategyMap");
					variableNameToStrategyMapContents.setAccessible(true);
					variableNameToStrategyMapContents.set(scoringSingletonObj, getVariableNameToStrategyMapContents());
					
					Set<Integer> modelIdsList = scoringSingletonObj.getModelIdList(newChangesVarValueMap);
				
					Map<String, Object> memberVariablesMap = scoringSingletonObj.createVariableValueMap("SearsTesting5", modelIdsList);
					Map<String, Change> changedMemberVariablesMap = scoringSingletonObj.createChangedVariablesMap("SearsTesting5");
					Map<String, Change> changedMemVariablesStrategy = scoringSingletonObj.executeStrategy(changedMemberVariablesMap, newChangesVarValueMap, memberVariablesMap);
					int value = (Integer) changedMemVariablesStrategy.get("S_DSL_APP_INT_ACC").getValue();
					List<Double> newScoreListActual = new LinkedList<Double>();
					for(int modelId: modelIdsList){
					double newScore = scoringSingletonObj.calcScore(memberVariablesMap, changedMemVariablesStrategy,modelId);
					newScoreListActual.add(newScore);
				}
					List<Double> newScoreListExpected = new LinkedList<Double>();
					newScoreListExpected.add(0.9935595238515038);
					newScoreListExpected.add(0.9525741268224333);
					newScoreListExpected.add(0.954291787707128);
					newScoreListExpected.add(0.9530462339457062);
					Assert.assertEquals(newScoreListExpected, newScoreListActual);
					Assert.assertEquals(1, value);
			}
	
	
	}
