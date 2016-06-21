package analytics.util.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Model;
import analytics.util.objects.RegionalFactor;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelSeasonalZipDao extends AbstractDao{
	DBCollection modelSeasonalZipCollection;
	 private Cache cache = null;
	    public ModelSeasonalZipDao(){
	    	super();
	    	modelSeasonalZipCollection = db.getCollection("modelSeasonalZip");
	    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SEASONAL_ZIP_FACTOR_CACHE);
	    	if(null == cache){
	    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SEASONAL_ZIP_FACTOR_CACHE);
	    		CacheBuilder.getInstance().setCaches(cache);
	    	}
	    }
		
		@SuppressWarnings("unchecked")
		public Map<String, RegionalFactor> getmodelSeasonalZipFactor(){
			String cacheKey = CacheConstant.RTS_MODEL_SEASONAL_ZIP_FACTOR_CACHE_KEY;
			Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
			if(element != null && element.getObjectKey().equals(cacheKey)){
				return (Map<String, RegionalFactor>) element.getObjectValue();
			}else{
				Map<String, RegionalFactor> modelCodeSeasonalZipFactorMap = new HashMap<String, RegionalFactor>();
			DBCursor vCursor = modelSeasonalZipCollection.find();
			for (DBObject dbObj: vCursor) {
				RegionalFactor regFactor = new RegionalFactor();
				regFactor.setZip((String)dbObj.get(MongoNameConstants.ZIP));
				regFactor.setF_date((String)dbObj.get(MongoNameConstants.EFF_DATE));
				regFactor.setFactor(Double.parseDouble(dbObj.get(MongoNameConstants.FACTOR).toString()));
				modelCodeSeasonalZipFactorMap.put(dbObj.get(MongoNameConstants.MODEL_ID).toString()+(String)dbObj.get(MongoNameConstants.ZIP), regFactor);
			}
			if(modelCodeSeasonalZipFactorMap != null && modelCodeSeasonalZipFactorMap.size() > 0){
				LOGGER.info("PERSIST: modelSeasonalZip collection gets refreshed on " + new Date());
				cache.put(new Element(cacheKey, (Map<String, RegionalFactor>) modelCodeSeasonalZipFactorMap));
			}
			return modelCodeSeasonalZipFactorMap;
			}
		}
}
