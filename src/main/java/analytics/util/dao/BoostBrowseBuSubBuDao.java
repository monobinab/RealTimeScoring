package analytics.util.dao;

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

public class BoostBrowseBuSubBuDao extends AbstractDao{
	
	 DBCollection boostBrowseBuSubBuCollection;
	 private Cache cache = null;
	    public BoostBrowseBuSubBuDao(){
	    	super();
	    	boostBrowseBuSubBuCollection = db.getCollection("boostBrowseBuSubBu");
	    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_BOOST_BROWSE_BUSUBBU_CACHE);
	    	if(null == cache){
	    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_BOOST_BROWSE_BUSUBBU_CACHE);
	    		CacheBuilder.getInstance().setCaches(cache);
	    	}
	    }
		
		@SuppressWarnings("unchecked")
		public Map<String, BoostBrowseBuSubBu> getBoostBuSubBuFromModelCode(){
			String cacheKey = CacheConstant.RTS_BOOST_BROWSE_BUSUBBU_CACHE_KEY;
			Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
			if(element != null && element.getObjectKey().equals(cacheKey)){
				return (Map<String, BoostBrowseBuSubBu>) element.getObjectValue();
			}else{
			Map<String, BoostBrowseBuSubBu> modelCodeToBoostBuSubBuMap = new HashMap<String, BoostBrowseBuSubBu>();
			DBCursor vCursor = boostBrowseBuSubBuCollection.find();
			for (DBObject dbObj: vCursor) {
				BoostBrowseBuSubBu boostBrowseBuSubBu = new BoostBrowseBuSubBu();
				boostBrowseBuSubBu.setBoost((String)dbObj.get(MongoNameConstants.BOOST));//BU_SUBBU
				boostBrowseBuSubBu.setBuSubBu((String)dbObj.get(MongoNameConstants.BU_SUBBU));
				modelCodeToBoostBuSubBuMap.put((String)dbObj.get(MongoNameConstants.MODEL_CODE), boostBrowseBuSubBu);
			}
			if(modelCodeToBoostBuSubBuMap != null && modelCodeToBoostBuSubBuMap.size() > 0){
				cache.put(new Element(cacheKey, (Map<String, BoostBrowseBuSubBu>) modelCodeToBoostBuSubBuMap));
			}
			return modelCodeToBoostBuSubBuMap;
			}
		}

	}