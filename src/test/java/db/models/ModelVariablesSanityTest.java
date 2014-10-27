package db.models;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class ModelVariablesSanityTest{
	@Test
	public void checkModelVariablesJsonFile() throws FileNotFoundException, IOException, ParseException{
		JSONParser parser = new JSONParser();

		String content = new String(Files.readAllBytes(Paths.get("db/models/modelVariables.json")), Charset.defaultCharset());
		Object obj = parser.parse("["+content+"]");

		JSONArray modelsArray = (JSONArray) obj;
		for(Object model: modelsArray){
			JSONObject modelObject = (JSONObject)model;
			Assert.assertTrue((Long)modelObject.get("modelId")>0);
			Assert.assertTrue((Long)modelObject.get("modelId")<76);
			String modelName = (String)modelObject.get("modelName");
			Assert.assertTrue(modelName.startsWith("S_SCR_")||modelName.startsWith("K_SCR_"));
		}
	}
}
