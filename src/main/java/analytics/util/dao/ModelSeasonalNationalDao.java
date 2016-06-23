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

public class ModelSeasonalNationalDao extends AbstractDao{

	DBCollection modelSeasonalNationalCollection;
	 private Cache cache = null;
	    public ModelSeasonalNationalDao(){
	    	super();
	    	modelSeasonalNationalCollection = db.getCollection("modelSeasonalNational");
	    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_NATIONAL_FACTOR_CACHE);
	    	if(null == cache){
	    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODEL_NATIONAL_FACTOR_CACHE);
	    		CacheBuilder.getInstance().setCaches(cache);
	    	}
	    }
		
		@SuppressWarnings("unchecked")
		public Map<Integer, RegionalFactor> getmodelSeasonalNationalFactor(){
			String cacheKey = CacheConstant.RTS_MODEL_NATIONAL_FACTOR_CACHE_KEY;
			Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
			if(element != null && element.getObjectKey().equals(cacheKey)){
				return (Map<Integer, RegionalFactor>) element.getObjectValue();
			}else{
				Map<Integer, RegionalFactor> modelCodeSeasonalNationalFactorMap = new HashMap<Integer, RegionalFactor>();
			DBCursor vCursor = modelSeasonalNationalCollection.find();
			for (DBObject dbObj: vCursor) {
				RegionalFactor regFactor = new RegionalFactor();
				regFactor.setF_date((String)dbObj.get(MongoNameConstants.EFF_DATE));
				regFactor.setFactor(Double.parseDouble(dbObj.get(MongoNameConstants.FACTOR).toString()));
				modelCodeSeasonalNationalFactorMap.put(Integer.parseInt(dbObj.get(MongoNameConstants.MODEL_ID).toString()), regFactor);
			}
			if(modelCodeSeasonalNationalFactorMap != null && modelCodeSeasonalNationalFactorMap.size() > 0){
				LOGGER.info("PERSIST: modelSeasonalNational collection gets refreshed on " + new Date());
				cache.put(new Element(cacheKey, (Map<Integer, RegionalFactor>) modelCodeSeasonalNationalFactorMap));
			}
			return modelCodeSeasonalNationalFactorMap;
			}
		}
}
