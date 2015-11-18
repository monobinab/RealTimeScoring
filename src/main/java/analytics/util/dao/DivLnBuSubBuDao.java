package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

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

public class DivLnBuSubBuDao extends AbstractDao{

    private DBCollection divLnBuSubBuCollection;
    private Cache cache = null;
    
    public DivLnBuSubBuDao(){
    	super();
    	divLnBuSubBuCollection = db.getCollection("divLnBuSubBu");
    	cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIV_LN_BU_SUBBU_CACHE);
    	CacheBuilder.getInstance().setCaches(cache);
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, String> getDvLnBuSubBu(){
    	String cacheKey = CacheConstant.RTS_DIV_LN_BU_SUBBU_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, String>) element.getObjectValue();
		}else{
    	Map<String, String> divLnBuSubBuMap = new HashMap<String, String>();
    	DBCursor cursor = divLnBuSubBuCollection.find();
    	while(cursor.hasNext()){
    		DBObject obj = cursor.next();
    		divLnBuSubBuMap.put(obj.get(MongoNameConstants.DIV_LN).toString(), obj.get(MongoNameConstants.BU_SUBBU).toString());
    	}
    	if(divLnBuSubBuMap != null && divLnBuSubBuMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, String>) divLnBuSubBuMap));
		}
    	return divLnBuSubBuMap;
		}
    }
}
