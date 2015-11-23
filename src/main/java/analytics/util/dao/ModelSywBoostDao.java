package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class ModelSywBoostDao extends AbstractDao {
    private DBCollection modelBoostCollection;
    private Cache cache = null;
    
    public ModelSywBoostDao(){
    	super();
    	modelBoostCollection = db.getCollection("modelSywBoosts");
    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SYW_BOOST_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SYW_BOOST_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, Integer> getVarModelMap(){
    	String cacheKey = CacheConstant.RTS_MODEL_SYW_BOOST_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, Integer>) element.getObjectValue();
		}else{
    	Map<String, Integer> varModelMap = new HashMap<String, Integer>();
    	DBCursor varModelCursor = modelBoostCollection.find();
    	for(DBObject varModelObj: varModelCursor) {
    		varModelMap.put((String) varModelObj.get("b"), (Integer) varModelObj.get("m"));
    	}
    	if(varModelMap != null && varModelMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, Integer>) varModelMap));
		}
    	return varModelMap;
		}
    }
    
	public Integer getModelId(String variable) {
		Map<String, Integer> varModelMap = this.getVarModelMap();
		if(StringUtils.isNotEmpty(variable)){
			return varModelMap.get(variable);
		}
		return null;
	}
}
