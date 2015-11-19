package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

public class DivLnBoostDao extends AbstractDao{
    private DBCollection divLnBoostCollection;
    private Cache cache = null;
    
    public DivLnBoostDao(){
    	super();
		divLnBoostCollection = db.getCollection("divLnBoost");
		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIV_LN_BOOSTCACHE);
    	CacheBuilder.getInstance().setCaches(cache);
    }
    
    @SuppressWarnings("unchecked")
	public HashMap<String, List<String>> getDivLnBoost(){
    	String cacheKey = CacheConstant.RTS_DIV_LN_BOOST_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<String, List<String>>) element.getObjectValue();
		}else{
    	HashMap<String, List<String>> divLnBoostMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnBoostCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnBoostMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV)) == null)
            {
                List<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString());
                divLnBoostMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), varColl);
            }
            else
            {
                List<String> varColl = divLnBoostMap.get(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString());
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString().toUpperCase());
                divLnBoostMap.put(divLnDBObject.get(MongoNameConstants.DLB_DIV).toString(), varColl);
            }
        }
    	if(divLnBoostMap != null && divLnBoostMap.size() > 0){
			cache.put(new Element(cacheKey, (HashMap<String, List<String>>) divLnBoostMap));
		}
    	return divLnBoostMap;
		}
    }
    
    /** Duplicate Method
    public HashMap<String, List<String>> getDivLnBoostVariable(){
    	HashMap<String, List<String>> divLnBoostVariablesMap = new HashMap<String, List<String>>();
    	DBCursor divLnVarCursor = divLnBoostCollection.find();
    	for(DBObject divLnDBObject: divLnVarCursor) {
            if (divLnBoostVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV)) == null)
            {
                List<String> varColl = new ArrayList<String>();
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString());
                divLnBoostVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
            else
            {
                List<String> varColl = divLnBoostVariablesMap.get(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString());
                varColl.add(divLnDBObject.get(MongoNameConstants.DLB_BOOST).toString().toUpperCase());
                divLnBoostVariablesMap.put(divLnDBObject.get(MongoNameConstants.DLV_DIV).toString(), varColl);
            }
        }
    	return divLnBoostVariablesMap;
    }*/
}
