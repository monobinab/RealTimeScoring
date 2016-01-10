package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Variable;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class VariableDao extends AbstractDao{
	
    private DBCollection variablesCollection;
    private Cache cache = null;
    private Cache allVariableCache = null;
    private BoostsDao boostsDao;
    
    public VariableDao(){
    	super();
    	boostsDao = new BoostsDao();
		variablesCollection = db.getCollection("Variables_sep");
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_VARIABLESCACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_VARIABLESCACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
    	allVariableCache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_CACHE);
    	if(null == allVariableCache){
    		allVariableCache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_ALL_VARIABLE_CACHE);
	    	CacheBuilder.getInstance().setCaches(allVariableCache);
    	}
    }
	
    @SuppressWarnings("unchecked")
	public List<Variable> getVariables() {
    	String cacheKey = CacheConstant.RTS_VARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<Variable>) element.getObjectValue();
		}else{
			List<Variable> variables = new ArrayList<Variable>();
			DBCursor vCursor = variablesCollection.find();
			for (DBObject variable : vCursor) {
				variables.add(new Variable(
						((DBObject) variable).get(MongoNameConstants.V_NAME).toString().toUpperCase(),
						((DBObject) variable).get(MongoNameConstants.V_ID).toString(),
						((DBObject) variable).get(MongoNameConstants.V_STRATEGY).toString()));
			}
			if(variables != null && variables.size() > 0){
				cache.put(new Element(cacheKey, (List<Variable>) variables));
			}
			return variables;
		}
	}
    
    @SuppressWarnings("unchecked")
	public List<Variable> getAllVariables(){
    	String cacheKey = CacheConstant.RTS_ALL_VARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(allVariableCache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<Variable>) element.getObjectValue();
		}else{
			List<Variable> variablesList = this.getVariables();
			List<Variable> boostsList = boostsDao.getBoosts();
			//Combining boosts with variables
			variablesList.addAll(boostsList);
			if(variablesList.size() != 0){
				allVariableCache.put(new Element(cacheKey, variablesList));
			}
			return variablesList;
		}
	  }
    
    
    
	
	public List<String> getVariableNames() {
		List<String> variables = new ArrayList<String>();
		List<Variable> vars = this.getVariables();
		if(vars != null && vars.size() > 0){
			for(Variable variable : vars){
				variables.add(variable.getName().toUpperCase());
			}
		}
		return variables;
	}
	
	public Set<String> getStrategyList() {
		Set<String> strategyList = new HashSet<String>();
		List<Variable> variables = this.getAllVariables();
		if(variables != null && variables.size() > 0){
			for(Variable variable : variables){
				if(!"NONE".equals(variable.getStrategy())) {
					strategyList.add(variable.getStrategy());
				}
			}
		}
		return strategyList;
	}
	
	public List<String> getBoostBrowseVariables() {
		List<String> variables = getVariableNames();
		List<String> boostVariables = new ArrayList<String>();
		for(String varName : variables){
			if(varName.contains(MongoNameConstants.BROWSE_BOOST_PREFIX)){
				boostVariables.add(varName);
			}
		}
			return boostVariables;
	}
}
