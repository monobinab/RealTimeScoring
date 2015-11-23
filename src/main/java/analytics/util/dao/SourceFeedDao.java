package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class SourceFeedDao extends AbstractDao{

	private DBCollection sourceFeedCollection;
	private Cache cache = null;

	public SourceFeedDao() {
		sourceFeedCollection = db.getCollection("sourceFeed");
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_SOURCEFEED_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_SOURCEFEED_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, String> getSourceFeedMap(){
		String cacheKey = CacheConstant.RTS_SOURCE_FEED_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, String>) element.getObjectValue();
		}else{
		Map<String, String> sourceFeedMap = new HashMap<String, String>();
		DBObject dbObj =  sourceFeedCollection.findOne();
		for(String key:dbObj.keySet()){
			sourceFeedMap.put(key, dbObj.get(key).toString());
		}
		if(sourceFeedMap != null && sourceFeedMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, String>) sourceFeedMap));
		}
		return sourceFeedMap;
		}
	}
}
