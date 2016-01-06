package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.BoostBrowseBuSubBu;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnModelCodeDao extends AbstractDao {

	DBCollection divLnModelCodeCollection;
	private Cache cache = null;
    public DivLnModelCodeDao(){
    	super();
    	divLnModelCodeCollection = db.getCollection("divLnModelCode");
    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_DIVlN_MODELCODE_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIVlN_MODELCODE_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    @SuppressWarnings("unchecked")
	public Map<String, List<String>> getDivLnModelCode(){
    	String cacheKey = CacheConstant.RTS_DIVLN_MODELCODE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, List<String>>) element.getObjectValue();
		}else{
    	HashMap<String, List<String>> divLnModelCodeMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnModelCodeCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnModelCodeMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV)) == null)
            {
                List<String> modelCode = new ArrayList<String>();
                modelCode.add(divLnDBObject.get(MongoNameConstants.MODEL_CODE).toString());
                divLnModelCodeMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), modelCode);
            }
            else
            {
                List<String> modelCode = divLnModelCodeMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString());
                modelCode.add(divLnDBObject.get(MongoNameConstants.MODEL_CODE).toString().toUpperCase());
                divLnModelCodeMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), modelCode);
            }
        }
    	if(divLnModelCodeMap != null && divLnModelCodeMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, List<String>>) divLnModelCodeMap));
		}
    		return divLnModelCodeMap;
		}
    }
}
