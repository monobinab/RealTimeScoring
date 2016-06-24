package analytics.util.dao;

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

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelSeasonalConstantDao extends AbstractDao{
	 DBCollection modelSeasonalConstantCollection;
	 private Cache cache = null;
	    public ModelSeasonalConstantDao(){
	    	super();
	    	modelSeasonalConstantCollection = db.getCollection("modelSeasonalConstant");
	    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SEASONAL_CONSTANT_CACHE);
	    	if(null == cache){
	    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_MODEL_SEASONAL_CONSTANT_CACHE);
	    		CacheBuilder.getInstance().setCaches(cache);
	    	}
	    }
		
		@SuppressWarnings("unchecked")
		public Map<Integer, Model> getmodelSeasonalConstant(){
			String cacheKey = CacheConstant.RTS_MODEL_SEASONAL_CONSTANT_CACHE_KEY;
			Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
			if(element != null && element.getObjectKey().equals(cacheKey)){
				return (Map<Integer, Model>) element.getObjectValue();
			}else{
				Map<Integer, Model> modelCodeSeasonalConstantMap = new HashMap<Integer, Model>();
			DBCursor vCursor = modelSeasonalConstantCollection.find();
			for (DBObject dbObj: vCursor) {
				Model model = new Model(Integer.parseInt(dbObj.get(MongoNameConstants.MODEL_ID).toString()), (String)dbObj.get(MongoNameConstants.MODEL_NAME), (String)MongoNameConstants.MODEL_CODE, Double.parseDouble((dbObj.get(MongoNameConstants.SEASONAL_CONSTANT).toString())));
				modelCodeSeasonalConstantMap.put(Integer.parseInt((String)dbObj.get(MongoNameConstants.MODEL_ID).toString()), model);
			}
			if(modelCodeSeasonalConstantMap != null && modelCodeSeasonalConstantMap.size() > 0){
				cache.put(new Element(cacheKey, (Map<Integer, Model>) modelCodeSeasonalConstantMap));
			}
			return modelCodeSeasonalConstantMap;
			}
		}
}
