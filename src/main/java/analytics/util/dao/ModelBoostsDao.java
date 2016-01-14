package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Boost;
import analytics.util.objects.Model;
import analytics.util.objects.ModelBoosts;
import analytics.util.objects.Variable;

import com.mongodb.BasicDBList;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelBoostsDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ModelBoostsDao.class);
    DBCollection modelBoostsCollection;
    private Cache cache = null;
    public ModelBoostsDao(){
    	super();
    	modelBoostsCollection = db.getCollection("modelBoost");
    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_BOOST_VARIABLES_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODEL_BOOST_VARIABLES_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }

   
    
	 @SuppressWarnings("unchecked")
	 public List<ModelBoosts> getModelBoostsList()   {
		 List<ModelBoosts> modelBoostsList = new ArrayList<ModelBoosts>();
		 String cacheKey = CacheConstant.RTS_MODEL_BOOSTS_CACHE_KEY;
			Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
			if(element != null && element.getObjectKey().equals(cacheKey)){
				return (List<ModelBoosts>) element.getObjectValue();
				}else{
				   DBCursor modelBoosts = modelBoostsCollection.find();
					
				 	for (DBObject modelBoost : modelBoosts) {
				 		ModelBoosts modelBoostObj = new ModelBoosts();
				 		int modelId = Integer.valueOf(modelBoost.get(MongoNameConstants.MODEL_ID).toString());
				 		String modelName = modelBoost.get(MongoNameConstants.MODEL_NAME).toString();
				 		modelBoostObj.setModelId(modelId);
				 		modelBoostObj.setModelName(modelName);
				 		
				 		List<Boost> boostsList = new ArrayList<Boost>();
				 		BasicDBList boostsDBList = (BasicDBList) modelBoost.get("variable");
				 		for (Object boostVariable : boostsDBList) {
				 			String variableName = ((DBObject) boostVariable).get(MongoNameConstants.VAR_NAME)
									.toString().toUpperCase();
				 			Double coefficient = Double.valueOf(((DBObject) boostVariable)
									.get(MongoNameConstants.COEFFICIENT).toString());
							Double intercept = Double.valueOf(((DBObject) boostVariable)
									.get(MongoNameConstants.INTERCEPT).toString());
							Boost boost = new Boost(variableName, coefficient, intercept);
							boostsList.add(boost);
				 		}
				 		modelBoostObj.setBoostLists(boostsList);
				 		modelBoostsList.add(modelBoostObj);
				 	}
				}
				if(modelBoostsList != null && modelBoostsList.size() > 0 ){
					cache.put(new Element(cacheKey, (List<ModelBoosts>)modelBoostsList));
				}
			return 	modelBoostsList;
	 	}
 
 
	 public Map<Integer, Model> getModelBoosts(){
		 
		 List<ModelBoosts> modelBoostsList = getModelBoostsList();
		 Map<Integer, Model> modelBoostsMap = new HashMap<Integer, Model>();
		 for(ModelBoosts modelBoost : modelBoostsList){
			 Map<String, Variable> boostsMap = new HashMap<String, Variable>();
			 List<Boost> boostsList = modelBoost.getBoostLists();
			 for(Boost boost : boostsList){
				 boostsMap.put(boost.getName(), new Boost(boost.getName(), boost.getCoefficient(), boost.getIntercept()));
			 }
			 modelBoostsMap.put(modelBoost.getModelId(), new Model(modelBoost.getModelId(), modelBoost.getModelName(), boostsMap));
		 }
		 
		 LOGGER.info("modelBoostsMap is populated from modelBoosts collection");
		return modelBoostsMap;
	 }
 
 
	 public Map<String, List<Integer>> getBoostModelsMap(){
		 List<ModelBoosts> modelBoostsList = getModelBoostsList();
		 Map<String, List<Integer>> boostModelsMap = new HashMap<String, List<Integer>>();
		 for(ModelBoosts modelBoost : modelBoostsList){
			 List<Boost> boostsList = modelBoost.getBoostLists();
			 for(Boost boost : boostsList){
					 
				 if (!boostModelsMap.containsKey(boost.getName())) {
						List<Integer> modelIds = new ArrayList<Integer>();
						boostModelsMap.put(boost.getName().toUpperCase(), modelIds);
						boostModelsMap.get(boost.getName().toUpperCase()).add(modelBoost.getModelId());
					} else {
						if (!boostModelsMap.get(boost.getName()).contains(modelBoost.getModelId())) {
							boostModelsMap.get(boost.getName().toUpperCase())
									.add(modelBoost.getModelId());
						}
					}
			 }
		 }
		return boostModelsMap;
	 }
 
 public void populateModelBoostMap(Map<Integer, Model> modelBoostsMap, Map<String, List<Integer>> boostModelsMap){
	 
	 List<ModelBoosts> modelBoostsList = getModelBoostsList();
	 for(ModelBoosts modelBoost : modelBoostsList){
		 Map<String, Variable> boostMap = populateBoostMap(modelBoost, boostModelsMap); 
		 modelBoostsMap.put(modelBoost.getModelId(), new Model(modelBoost.getModelId(), modelBoost.getModelName(), boostMap));
	 }
 }
 
 public Map<String, Variable> populateBoostMap(ModelBoosts modelboost, Map<String, List<Integer>> boostModelsMap){
	 
	 Map<String, Variable> boostsMap = new HashMap<String, Variable>();
	 List<Boost> boostsList = modelboost.getBoostLists();
	 for(Boost boost : boostsList){
		 boostsMap.put(boost.getName(), new Boost(boost.getName(), boost.getCoefficient(), boost.getIntercept()));
		 
		 if (!boostModelsMap.containsKey(boost.getName())) {
				List<Integer> modelIds = new ArrayList<Integer>();
				boostModelsMap.put(boost.getName().toUpperCase(), modelIds);
				boostModelsMap.get(boost.getName().toUpperCase()).add(modelboost.getModelId());
			} else {
				if (!boostModelsMap.get(boost.getName()).contains(modelboost.getModelId())) {
					boostModelsMap.get(boost.getName().toUpperCase())
							.add(modelboost.getModelId());
				}
			}
	 }
	 return boostsMap;
 }
 
 
 
 
 /*public void populateModelBoostsMap(Map<Integer, Model> modelBoostsMap, Map<String, List<Integer>> boostModelsMap){
	DBCursor modelBoosts = modelBoostsCollection.find();

	for (DBObject model : modelBoosts) {
		int modelId = Integer.valueOf(model.get(MongoNameConstants.MODEL_ID).toString());
		String modelName = model.get(MongoNameConstants.MODEL_NAME).toString();
		Map<String, Variable> boostMap = populateBoostModelsMap(
				model, modelId, boostModelsMap);
	
			modelBoostsMap.put(modelId, new Model(modelId, modelName,  boostMap));
	}
}*/

/* private Map<String, Variable> populateBoostModelsMap(DBObject model,
		int modelId, Map<String, List<Integer>> boostModelsMap) {
	BasicDBList modelVariables = (BasicDBList) model.get(MongoNameConstants.VARIABLE);
	Map<String, Variable> variablesMap = new HashMap<String, Variable>();
	for (Object modelVariable : modelVariables) {
		String variableName = ((DBObject) modelVariable).get(MongoNameConstants.VAR_NAME)
				.toString().toUpperCase();
	
			Double coefficient = Double.valueOf(((DBObject) modelVariable)
					.get(MongoNameConstants.COEFFICIENT).toString());
			Double intercept = Double.valueOf(((DBObject) modelVariable)
					.get(MongoNameConstants.INTERCEPT).toString());
			variablesMap.put(variableName, new Boost(variableName,coefficient,intercept));
	
		if (!boostModelsMap.containsKey(variableName)) {
			List<Integer> modelIds = new ArrayList<Integer>();
			boostModelsMap.put(variableName.toUpperCase(), modelIds);
			boostModelsMap.get(variableName.toUpperCase()).add(modelId);
		} else {
			if (!boostModelsMap.get(variableName).contains(modelId)) {
				boostModelsMap.get(variableName.toUpperCase())
						.add(modelId);
			}
		}
	}
	return variablesMap;
}
*/
}