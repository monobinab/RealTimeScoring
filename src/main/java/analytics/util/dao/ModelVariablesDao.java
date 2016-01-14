package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Boost;
import analytics.util.objects.Model;
import analytics.util.objects.ModelVariable;
import analytics.util.objects.Variable;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class ModelVariablesDao extends AbstractDao{
    
	private DBCollection modelVariablesCollection;
	private Cache cache = null;
	private Cache allModelVariablesCache= null;
	private Cache allVariableModelsCache = null;
	private ModelBoostsDao modelBoostsDao;
	private VariableDao variableDao;
    
    public ModelVariablesDao(){
    	super();
		modelVariablesCollection = db.getCollection("modelVariables");
		modelBoostsDao = new ModelBoostsDao();
		variableDao = new VariableDao();
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODELVARIABLESCACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODELVARIABLESCACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    	allModelVariablesCache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_MODEL_VARIABLES_CACHE);
    	if(null == allModelVariablesCache){
    		allModelVariablesCache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_ALL_MODEL_VARIABLES_CACHE);
    		CacheBuilder.getInstance().setCaches(allModelVariablesCache);
    	}
    	allVariableModelsCache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_MODELS_CACHE);
    	if(null == allVariableModelsCache){
    		allVariableModelsCache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_MODELS_CACHE);
    		CacheBuilder.getInstance().setCaches(allVariableModelsCache);
    	}
    }
    
	@SuppressWarnings("unchecked")
	private List<ModelVariable> getModelVariables(){
		String cacheKey = CacheConstant.RTS_MODEL_VAR_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<ModelVariable>) element.getObjectValue();
		}else{
		List<ModelVariable> modelVariables = new ArrayList<ModelVariable>();
		DBCursor dbCursor = modelVariablesCollection.find();
		if(dbCursor != null && dbCursor.count() > 0){
			while(dbCursor.hasNext()) {
				DBObject dbObj = dbCursor.next();
				if(dbObj != null){
					ModelVariable modelVariable = new ModelVariable();
					modelVariable.setModelId((Integer)dbObj.get("modelId"));
					modelVariable.setModelName((String)dbObj.get("modelName"));
					modelVariable.setModelDescription((String)dbObj.get("modelDescription"));
					if(dbObj.get("constant") instanceof Integer){
						int val = (Integer)dbObj.get("constant");
						modelVariable.setConstant(val*1.0);
					}else if(dbObj.get("constant") instanceof Double){
						modelVariable.setConstant((Double)dbObj.get("constant"));
					}
					modelVariable.setMonth((Integer)dbObj.get("month"));
					
					BasicDBList variableList = (BasicDBList)dbObj.get("variable");
					if(variableList != null && variableList.size() > 0){
						List<Variable> dbVariables = new ArrayList<Variable>();
						for(Iterator<Object> it = variableList.iterator(); it.hasNext();){
							BasicDBObject dbo = (BasicDBObject) it.next();
							Variable variable = new Variable();
								if(dbo != null && dbo.containsField("name")){
									variable.setName((String)dbo.get("name"));
									if(dbo.get("coefficient") instanceof Integer){
										int val = (Integer)dbo.get("coefficient");
										variable.setCoefficient(val*1.0);
									}else if(dbo.get("coefficient") instanceof Double){
										variable.setCoefficient((Double)dbo.get("coefficient"));
									}
									if(dbo.get("intercept") != null){
										if(dbo.get("intercept") instanceof Integer){
											int val = (Integer)dbo.get("intercept");
											variable.setIntercept(val*1.0);
										}else if(dbo.get("intercept") instanceof Double){
											variable.setIntercept((Double)dbo.get("intercept"));
										}
									}
									int defaultValue = variableDao.getDefaultValue((String)dbo.get("name"));
									if(defaultValue != 0){
										variable.setDefaultValue(defaultValue);
									}
								}
								dbVariables.add(variable);
							}
						modelVariable.setVariable(dbVariables);
						modelVariables.add(modelVariable);
						}
					}
				}
			}
		if(modelVariables != null && modelVariables.size() > 0){
			cache.put(new Element(cacheKey, (List<ModelVariable>) modelVariables));
		}
			return modelVariables;
		}
	}
	
	public Map<Integer, Map<Integer, Model>> getModelsMap(){
		List<ModelVariable> modelVariables = this.getModelVariables();
		Map<Integer, Map<Integer, Model>> modelsMap = new HashMap<Integer, Map<Integer,Model>>();
    	if(modelVariables != null && modelVariables.size() > 0){
			for(ModelVariable modelVariable : modelVariables){
				int modelId = modelVariable.getModelId();
				String modelName = modelVariable.getModelName();
				int month = modelVariable.getMonth();
				double constant = modelVariable.getConstant();
				List<Variable> variables = modelVariable.getVariable();
				
				Map<String, Variable> variablesMap = new HashMap<String, Variable>();
				for (Variable variable : variables) {
					String variableName = variable.getName().toUpperCase();
						Double coefficient = variable.getCoefficient();
						variablesMap.put(variableName, new Variable(variableName,coefficient, variable.getDefaultValue()));
					}
				
				if (!modelsMap.containsKey(modelId)) {
					Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
					monthModelMap.put(month, new Model(modelId, modelName, month, constant, variablesMap));
					modelsMap.put(modelId, monthModelMap);
				} else {
					modelsMap.get(modelId).put(month, new Model(modelId, modelName, month, constant, variablesMap));
				}
    		}
    	}
    	LOGGER.info("modelsMap is populated from modelVariables collection");
		return modelsMap;
	}
	
	
	public Map<String, List<Integer>> getVariableModelsMap(){
		List<ModelVariable> modelVariables = this.getModelVariables();
		Map<String, List<Integer>> variableModelsMap = new HashMap<String, List<Integer>>();
    	if(modelVariables != null && modelVariables.size() > 0){
			for(ModelVariable modelVariable : modelVariables){
				int modelId = modelVariable.getModelId();
				List<Variable> variables = modelVariable.getVariable();
				for (Variable variable : variables) {
					String variableName = variable.getName().toUpperCase();
					   if (!variableModelsMap.containsKey(variableName)) {
							List<Integer> modelIds = new ArrayList<Integer>();
							variableModelsMap.put(variableName.toUpperCase(), modelIds);
							variableModelsMap.get(variableName.toUpperCase()).add(modelId);
						} else {
							if (!variableModelsMap.get(variableName).contains(modelId)) {
								variableModelsMap.get(variableName.toUpperCase()).add(modelId);
							}
						}
				 }
			}
    	}
		return variableModelsMap;
	}
    
    
	@SuppressWarnings("unchecked")
	public Map<Integer, Map<Integer, Model>> getAllModelVariables(){
		
		String cacheKey = CacheConstant.RTS_ALL_MODEL_VARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(allModelVariablesCache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<Integer, Map<Integer, Model>>) element.getObjectValue();
		}else{
			Map<Integer, Map<Integer, Model>> modelsMap = this.getModelsMap();
			Map<Integer, Model> modelBoostsMap = modelBoostsDao.getModelBoosts();
			//Combining modelsMap and modelBoostsMap
			for (Entry<Integer, Map<Integer, Model>> modelVarsMap : modelsMap.entrySet())
			{
				Map<Integer, Model> monthModelMap = modelVarsMap.getValue();
				for (Entry<Integer, Model> modelMap : monthModelMap.entrySet())
				{
					Model model = modelMap.getValue();
					int modelId = model.getModelId();
					String modelName = model.getModelName();
					double constant = model.getConstant();
					int month = model.getMonth();
					Map<String, Variable> vars = model.getVariables();
					Model boostsModel = modelBoostsMap.get(modelId);
					if(boostsModel != null){
						Map<String, Variable> boost = boostsModel.getVariables();
						vars.putAll(boost);
						modelsMap.get(modelId).put(month, new Model(modelId, modelName, month, constant, vars ));
					}
				}
			}
			
			if(modelsMap != null && modelsMap.size() > 0){
				allModelVariablesCache.put(new Element(cacheKey, (Map<Integer, Map<Integer, Model>>) modelsMap));
			}
				return modelsMap;
		}
	}
	
    
	@SuppressWarnings("unchecked")
	public Map<String, List<Integer>> getAllVariableModelsMap(){
		String cacheKey = CacheConstant.RTS_ALL_VARIABLE_MODEL_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(allVariableModelsCache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, List<Integer>>) element.getObjectValue();
		}else{
			Map<String, List<Integer>> variableModelsMap = this.getVariableModelsMap();
			Map<String, List<Integer>> boostModelsMap = modelBoostsDao.getBoostModelsMap();
			//Combining variableModelsMap and boostModelsMap
			variableModelsMap.putAll(boostModelsMap);
			
			if(variableModelsMap != null && variableModelsMap.size() > 0){
				allVariableModelsCache.put(new Element(cacheKey, (Map<String, List<Integer>>) variableModelsMap));
			}
				return variableModelsMap;
		}
	}
	
	
	
	
    
    //////////////
	public List<String> getVariableList(){
    	List<String> modelVariablesList = new ArrayList<String>();
    	List<ModelVariable> modelVariables = this.getModelVariables();
    	if(modelVariables != null && modelVariables.size() > 0){
    		for(ModelVariable modelVariable : modelVariables){
    			 List<Variable> variables = modelVariable.getVariable();
    			 if(variables != null && variables.size() > 0){
    				 for(Variable variable : variables){
    					 modelVariablesList.add(variable.getName());
    				 }
    			 }
    		}
    	}
		return modelVariablesList;
	}

    public void populateModelVariables(Map<Integer, Map<Integer, Model>> modelsMap, Map<String, List<Integer>> variableModelsMap){
    	List<ModelVariable> modelVariables = this.getModelVariables();
    	if(modelVariables != null && modelVariables.size() > 0){
			for(ModelVariable modelVariable : modelVariables){
				int modelId = modelVariable.getModelId();
				String modelName = modelVariable.getModelName();
				int month = modelVariable.getMonth();
				double constant = modelVariable.getConstant();
				List<Variable> variables = modelVariable.getVariable();
				Map<String, Variable> variablesMap = populateVariableModelsMap(variables, modelId, variableModelsMap);		
				if (!modelsMap.containsKey(modelId)) {
					Map<Integer, Model> monthModelMap = new HashMap<Integer, Model>();
					monthModelMap.put(month, new Model(modelId, modelName, month, constant, variablesMap));
					modelsMap.put(modelId, monthModelMap);
				} else {
					modelsMap.get(modelId).put(month, new Model(modelId, modelName, month, constant, variablesMap));
				}
    		}
    	}
    }
       
	private Map<String, Variable> populateVariableModelsMap(List<Variable> variables, int modelId, Map<String, List<Integer>> variableModelsMap) {
		Map<String, Variable> variablesMap = new HashMap<String, Variable>();
		for (Variable variable : variables) {
			String variableName = variable.getName().toUpperCase();
			if(variableName.substring(0,5).toUpperCase().equals(MongoNameConstants.BOOST_VAR_PREFIX)) {
				Double coefficient = variable.getCoefficient();
				Double intercept = variable.getIntercept();
				variablesMap.put(variableName, new Boost(variableName, coefficient, intercept));
			} else {
				Double coefficient = variable.getCoefficient();
				variablesMap.put(variableName, new Variable(variableName,coefficient, variable.getDefaultValue() ));
			}
			if (!variableModelsMap.containsKey(variableName)) {
				List<Integer> modelIds = new ArrayList<Integer>();
				variableModelsMap.put(variableName.toUpperCase(), modelIds);
				variableModelsMap.get(variableName.toUpperCase()).add(modelId);
			} else {
				if (!variableModelsMap.get(variableName).contains(modelId)) {
					variableModelsMap.get(variableName.toUpperCase()).add(modelId);
				}
			}
		}
		return variablesMap;
	}
	

	
}