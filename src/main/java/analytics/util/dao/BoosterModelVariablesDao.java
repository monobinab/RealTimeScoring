package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.BoosterModel;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class BoosterModelVariablesDao extends AbstractDao {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(BoosterModelVariablesDao.class);

    DBCollection boosterModelVariablesCollection;
    
    public BoosterModelVariablesDao(){
    	super();
    	boosterModelVariablesCollection = db.getCollection("boosterModelVariables");
    }
    
    public void populateBoosterModelVariables(Set<Integer> boosterModelIds, Map<Integer, BoosterModel> boosterModelVariablesMap){
    	
    	DBCursor boosterModels = boosterModelVariablesCollection.find();
    	for(DBObject boosterModelDbObj:boosterModels){
    		BoosterModel boosterModel = new BoosterModel();
    		int modelId = Integer.valueOf(boosterModelDbObj.get(MongoNameConstants.MODEL_ID).toString());
			String modelName = boosterModelDbObj.get(MongoNameConstants.MODEL_NAME).toString();
			double constant = Double.valueOf(boosterModelDbObj.get(MongoNameConstants.CONSTANT).toString());

			Map<String, Double> boosterVariablesMap = populateBoosterVariables(boosterModelDbObj);

			boosterModel.setModelId(modelId);
			boosterModel.setModelName(modelName);
			boosterModel.setConstant(constant);
			boosterModel.setBoosterVariablesMap(boosterVariablesMap);
			boosterModelVariablesMap.put(modelId, boosterModel);

			boosterModelIds.add(modelId);
		}
    }
    
    public Map<String, Double> populateBoosterVariables(DBObject boosterModel){
    	Map<String, Double> boosterVariablesMap = new HashMap<String, Double>();
    	if(boosterModel != null){
    	BasicDBList modelVariables = (BasicDBList) boosterModel.get(MongoNameConstants.VARIABLE);
    	for(Object varObj:modelVariables){
    		String varName = ((DBObject) varObj).get(MongoNameConstants.VAR_NAME).toString();
    		Double coefficient = Double.valueOf(((DBObject) varObj)
					.get(MongoNameConstants.COEFFICIENT).toString());
    		boosterVariablesMap.put(varName, coefficient);
    		}
    	}
		return boosterVariablesMap;
    }
}