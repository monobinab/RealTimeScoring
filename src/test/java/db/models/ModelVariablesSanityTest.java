package db.models;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

import analytics.util.dao.MemberVariablesDao;
import analytics.util.dao.ModelVariablesDao;
import analytics.util.dao.ModelsDao;
import analytics.util.dao.VariableDao;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

/**
 * This test connects to PROD mongo DB for read operations
 * 
 *
 */
public class ModelVariablesSanityTest{
	//NOTE: IF Jenkins is taking too long to run at every checkin, move this to integration or hourly tests
	@Test
	public void checkModelVariables() throws FileNotFoundException, IOException, ParseException{
		//Read the json file that is checked in
		JSONParser parser = new JSONParser();
		String content = new String(Files.readAllBytes(Paths.get("db/models/modelVariables.json")), Charset.defaultCharset());
		//Create a json array
		Object obj = parser.parse("["+content+"]");
		JSONArray modelsArray = (JSONArray) obj;

		//Initiatize maps to store mongo results
		Map<Integer, Map<Integer, Model>> modelVariablesMap = new HashMap<Integer, Map<Integer,Model>>();
		Map<String, List<Integer>> variableModelsMap = new HashMap<String, List<Integer>>();
		
		
		
		//Actually connect to PROD! mongo
		System.setProperty("rtseprod", "true");
		ModelVariablesDao modelVariablesDao = new ModelVariablesDao();
		modelVariablesDao.populateModelVariables(modelVariablesMap, variableModelsMap);
		Map<Integer,String> modelNamesMap = new ModelsDao().getModelNames();
		List<String> variablesList = new VariableDao().getVariableNames();
		System.setProperty("rtseprod", "test");	
		
		//Change this everytime you add a new model
		Assert.assertEquals(128, modelsArray.size());
		Assert.assertEquals(40, modelVariablesMap.size());
		
		for(Integer modelId: modelVariablesMap.keySet()){
			Map<Integer, Model> model = modelVariablesMap.get(modelId);
			if(model.containsKey(0)){
				Assert.assertTrue(modelNamesMap.values().contains(model.get(0).getModelName()));
			}
			else{
				for(int i=1;i<=12;i++){
					//Should contain model for each month
					Assert.assertTrue(modelNamesMap.values().contains(model.get(i).getModelName()));
				}
					
			}
		}

		//Model + month is unique
		
		for(Object model: modelsArray){
			JSONObject modelObject = (JSONObject)model;
			Assert.assertTrue((Long)modelObject.get("modelId")>0);
			Assert.assertTrue((Long)modelObject.get("modelId")<76);
			String modelName = (String)modelObject.get("modelName");
			Assert.assertTrue(modelNamesMap.values().contains(modelName));
			Assert.assertNotNull((String)modelObject.get("modelDescription"));
			Assert.assertNotNull((Double)modelObject.get("constant"));
			JSONArray variables = (JSONArray)modelObject.get("variable");
			Assert.assertNotNull(variables);
			for(Object variable: variables){
				JSONObject variableObject = (JSONObject)variable;
				Assert.assertTrue(variablesList.contains(((String)variableObject.get("name")).toUpperCase()));
				Assert.assertNotNull((Double)variableObject.get("coefficient"));
			}
		}
	}
}
