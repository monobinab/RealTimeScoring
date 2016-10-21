package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.Variable;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class PurchaseDao extends AbstractDao{
	
	DBCollection purchaseCollection;
    private Cache cache = null;

    public PurchaseDao(){
        super();
        purchaseCollection = db.getCollection("Purchase");
        cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_PURCHASE_CACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_PURCHASE_CACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    @SuppressWarnings("unchecked")
	public List<Variable> getPurchaseVariables() {
		
		String cacheKey = CacheConstant.RTS_PURCHASE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		List<Variable> boosts = new ArrayList<Variable>();
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<Variable>) element.getObjectValue();
			}else{
				if(purchaseCollection != null){
					DBCursor vCursor = purchaseCollection.find();
					
					for (DBObject variable : vCursor) {
						boosts.add(new Variable(
								((DBObject) variable).get(MongoNameConstants.V_NAME).toString().toUpperCase(),
								((DBObject) variable).get(MongoNameConstants.V_ID).toString(),
								((DBObject) variable).get(MongoNameConstants.V_STRATEGY).toString()));
					}
				
				}
			}
			return boosts;
		}
    

}
