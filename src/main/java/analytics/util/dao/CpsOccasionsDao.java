/**
 * 
 */
package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.OccasionInfo;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author spannal
 *
 */
public class CpsOccasionsDao extends AbstractDao {
	
    private DBCollection cpsOccasionsCollection;
    private Cache cache = null;
    
    public CpsOccasionsDao(){
    	super();
    	cpsOccasionsCollection = db.getCollection("cpsOccasions");
    	cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_CPSOCCASIONSCACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_CPSOCCASIONSCACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
    }
    
    public HashMap<String, String> getcpsOccasionDurations(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
    	List<OccasionInfo> occasionInfos = this.getCPSOccasionInfo();
    	if(occasionInfos != null && occasionInfos.size() > 0){
    		for(OccasionInfo occasionInfo : occasionInfos){
    			cpsOccasionDurationsMap.put(occasionInfo.getOccasionId(), occasionInfo.getTagExpiresIn());
    		}
    	}
    	return cpsOccasionDurationsMap;
    }
    
    public HashMap<String, String> getcpsOccasionPriority(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
    	List<OccasionInfo> occasionInfos = this.getCPSOccasionInfo();
    	if(occasionInfos != null && occasionInfos.size() > 0){
    		for(OccasionInfo occasionInfo : occasionInfos){
    			cpsOccasionDurationsMap.put(occasionInfo.getOccasionId(), occasionInfo.getPriority());
    		}
    	}
    	return cpsOccasionDurationsMap;
    }
    
    public HashMap<String, String> getcpsOccasionId(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
    	List<OccasionInfo> occasionInfos = this.getCPSOccasionInfo();
    	if(occasionInfos != null && occasionInfos.size() > 0){
    		for(OccasionInfo occasionInfo : occasionInfos){
    			cpsOccasionDurationsMap.put(occasionInfo.getOccasion(), occasionInfo.getOccasionId());
    		}
    	}
    	return cpsOccasionDurationsMap;
    }
    
    @SuppressWarnings("unchecked")
	private List<OccasionInfo> getCPSOccasionInfo(){
    	String cacheKey = CacheConstant.RTS_CPSOCCASIONS_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<OccasionInfo>) element.getObjectValue();
		}else{
    	List<OccasionInfo> occasionInfos = new ArrayList<OccasionInfo>();
    	DBCursor cpsOccasionsCursor = cpsOccasionsCollection.find();
    	System.out.println(cpsOccasionsCursor.count());
    	if(cpsOccasionsCursor != null && cpsOccasionsCursor.count() > 0){
	    	while(cpsOccasionsCursor.hasNext()) {
	    		DBObject dbObj = cpsOccasionsCursor.next();
	    		if(dbObj != null){
	    		OccasionInfo occasionInfo = new OccasionInfo();
	    		occasionInfo.setOccasionId(dbObj.get(MongoNameConstants.OCCASIONID).toString());
	    		occasionInfo.setOccasion(dbObj.get(MongoNameConstants.OCCASION).toString());
	    		occasionInfo.setPriority(dbObj.get(MongoNameConstants.PRIORITY).toString());
	    		occasionInfo.setDuration(dbObj.get(MongoNameConstants.DURATION).toString());
	    		occasionInfo.setDaysToCheckInHistory(dbObj.get(MongoNameConstants.DAYSINHISTORY).toString());
	    		occasionInfo.setTagExpiresIn(dbObj.get(MongoNameConstants.TAGEXPIRESIN).toString());
	    		occasionInfos.add(occasionInfo);
	    		}
	    	}
    	}
    	if(occasionInfos != null && occasionInfos.size() > 0){
			cache.put(new Element(cacheKey, (List<OccasionInfo>) occasionInfos));
		}
    	return occasionInfos;
		}
    }
}
