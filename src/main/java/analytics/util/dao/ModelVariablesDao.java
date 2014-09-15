package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;
import analytics.util.objects.Model;
import analytics.util.objects.Variable;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelVariablesDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(ModelVariablesDao.class);
	static DB db;
    DBCollection modelVariablesCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public ModelVariablesDao(){
		modelVariablesCollection = db.getCollection("modelVariables");
    }
    public List<String> getVariableList(){
    	List<String> modelVariablesList = new ArrayList<String>();
    	DBCursor modelVariablesCursor = modelVariablesCollection.find();
		for(DBObject modelDBO:modelVariablesCursor) {
			BasicDBList variablesDBList = (BasicDBList) modelDBO.get(MongoNameConstants.MODELV_VARIABLE);
			for(Object var:variablesDBList) {
				if(!modelVariablesList.contains(var.toString())) {
					modelVariablesList.add(((BasicDBObject) var).get(MongoNameConstants.MODELV_NAME).toString());
				}
			}
		}
    	return modelVariablesList;
    }
    

    public void populateModelVariables(Map<Integer, Map<Integer, Model>> modelsMap,
    		Map<String, List<Integer>> variableModelsMap){
		DBCursor models = modelVariablesCollection.find();
		for (DBObject model : models) {
			int modelId = Integer.valueOf(model.get(MongoNameConstants.MODEL_ID).toString());
			int month = Integer.valueOf(model.get(MongoNameConstants.MONTH).toString());
			double constant = Double.valueOf(model.get(MongoNameConstants.CONSTANT).toString());
	
			Map<String, Variable> variablesMap = populateVariableModelsMap(
					model, modelId,variableModelsMap);
			
			if (!modelsMap.containsKey(modelId)) {
				Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
				monthModelMap.put(month, new Model(modelId, month, constant,
						variablesMap));
				modelsMap.put(modelId, monthModelMap);
			} else {
				modelsMap.get(modelId).put(month,
						new Model(modelId, month, constant, variablesMap));
			}
		}
    }
    
	private Map<String, Variable> populateVariableModelsMap(DBObject model,
			int modelId, Map<String, List<Integer>> variableModelsMap) {
		BasicDBList modelVariables = (BasicDBList) model.get(MongoNameConstants.VARIABLE);
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		for (Object modelVariable : modelVariables) {
			String variableName = ((DBObject) modelVariable).get(MongoNameConstants.VAR_NAME)
					.toString().toUpperCase();
			Double coefficient = Double.valueOf(((DBObject) modelVariable)
					.get(MongoNameConstants.COEFFICIENT).toString());
			variablesMap.put(variableName, new Variable(variableName,coefficient));

			if (!variableModelsMap.containsKey(variableName)) {
				List<Integer> modelIds = new ArrayList<Integer>();
				variableModelsMap.put(variableName.toUpperCase(), modelIds);
				variableModelsMap.get(variableName.toUpperCase()).add(modelId);
			} else {
				if (!variableModelsMap.get(variableName).contains(modelId)) {
					variableModelsMap.get(variableName.toUpperCase())
							.add(modelId);
				}
			}
		}
		return variablesMap;
	}
}
