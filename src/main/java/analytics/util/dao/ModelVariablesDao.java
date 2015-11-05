package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    
    public ModelVariablesDao(){
    	super();
		modelVariablesCollection = db.getCollection("modelVariables");
		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODELVARIABLESCACHE);
    	CacheBuilder.getInstance().setCaches(cache);
    }
    
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
				variablesMap.put(variableName, new Variable(variableName,coefficient));
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
					try{
						modelVariable.setModelId((Integer)dbObj.get("modelId"));
						modelVariable.setModelName((String)dbObj.get("modelName"));
						modelVariable.setModelDescription((String)dbObj.get("modelDescription"));
						modelVariable.setConstant((Double)dbObj.get("constant"));
						modelVariable.setMonth((Integer)dbObj.get("month"));
					}catch(ClassCastException ex){
						modelVariable.setModelId((Integer)dbObj.get("modelId"));
						modelVariable.setModelName((String)dbObj.get("modelName"));
						modelVariable.setModelDescription((String)dbObj.get("modelDescription"));
						int val = (Integer)dbObj.get("constant");
						modelVariable.setConstant(val*1.0);
						modelVariable.setMonth((Integer)dbObj.get("month"));
					}
					BasicDBList variableList = (BasicDBList)dbObj.get("variable");
					if(variableList != null && variableList.size() > 0){
						List<Variable> dbVariables = new ArrayList<Variable>();
						for(Iterator<Object> it = variableList.iterator(); it.hasNext();){
							BasicDBObject dbo = (BasicDBObject) it.next();
							Variable variable = new Variable();
							try{
								if(dbo != null && dbo.containsField("name")){
									variable.setName((String)dbo.get("name"));
									variable.setCoefficient((Double)dbo.get("coefficient"));
									if(dbo.get("intercept") != null){
										variable.setIntercept((Double)dbo.get("intercept"));
									}
									dbVariables.add(variable);
									}
								}catch(ClassCastException ex){
								int val = (Integer)dbo.get("coefficient");
								variable.setName((String)dbo.get("name"));
								variable.setCoefficient(val*1.0);
								if(dbo.get("intercept") != null){
									variable.setCoefficient((Double)dbo.get("intercept"));
								}
								dbVariables.add(variable);
							}
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
}