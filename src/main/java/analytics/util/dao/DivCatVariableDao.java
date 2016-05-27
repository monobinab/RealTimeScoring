package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivCatVariableDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivCatVariableDao.class);
    private DBCollection divCatVariableCollection;
	private Cache cache = null;
	
    public DivCatVariableDao(){
    	super();
    	if(db != null){
    		divCatVariableCollection = db.getCollection("divCatVariable");
    	}
    	else{
    		LOGGER.error("db NULL in DivCatVariableDao");
    	}
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_DIV_CAT_VARIABLECACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIV_CAT_VARIABLECACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    @SuppressWarnings("unchecked")
	public HashMap<String, List<String>> getDivCatVariable(){
    	String cacheKey = CacheConstant.RTS_DIVCATVARIABLE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<String, List<String>>) element.getObjectValue();
		}else{
    	HashMap<String, List<String>> divLnVariablesMap = new HashMap<String, List<String>>();
    	if(divCatVariableCollection != null){
	    	DBCursor divLnVarCursor = divCatVariableCollection.find();
	    	for(DBObject divLnDBObject: divLnVarCursor) {
	            if (divLnVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV)) == null)
	            {
	                List<String> varColl = new ArrayList<String>();
	                varColl.add(divLnDBObject.get(MongoNameConstants.DLV_VAR).toString());
	                divLnVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
	            }
	            else
	            {
	                List<String> varColl = divLnVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString());
	                varColl.add(divLnDBObject.get(MongoNameConstants.DLV_VAR).toString().toUpperCase());
	                divLnVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
	            }
	        }
    	}
    	if(divLnVariablesMap != null && divLnVariablesMap.size() > 0){
			cache.put(new Element(cacheKey, (HashMap<String, List<String>>) divLnVariablesMap));
		}
    	return divLnVariablesMap;
		}
    }
}
