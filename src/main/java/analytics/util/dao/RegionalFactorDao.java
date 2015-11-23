package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

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

public class RegionalFactorDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(RegionalFactorDao.class);
	private DBCollection regionalFactorColl;
	private Cache cache = null;
	
	public RegionalFactorDao() {
		super();
		regionalFactorColl = db.getCollection("regionalAdjustmentFactors"); 
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_REGIONALFACTORCACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_REGIONALFACTORCACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
	}

	@SuppressWarnings("unchecked")
	public  Map<String, Double> populateRegionalFactors(){
		String cacheKey = CacheConstant.RTS_REGIONALFACTOR_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, Double>) element.getObjectValue();
		}else{
		DBCursor cursor = regionalFactorColl.find();
		Map<String, Double> regionalFactorMap = new HashMap<String, Double>();
		while(cursor.hasNext()){
			DBObject dbObj = cursor.next();
			if(dbObj != null){
				String key = (String) dbObj.get(MongoNameConstants.MODEL_ID) + "-" + (String)dbObj.get(MongoNameConstants.REGIONAL_STATE);
				Double factor = Double.parseDouble((String)dbObj.get(MongoNameConstants.FACTOR));
				regionalFactorMap.put(key, factor);
			}
		}
		LOGGER.info("regionalFactor size is " + regionalFactorMap.size());
		if(regionalFactorMap != null && regionalFactorMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, Double>) regionalFactorMap));
		}
		return regionalFactorMap;
		}
	}
}
