package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.OccCustEvent;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OccationCustomeEventDao extends AbstractDao {
	private DBCollection occCustEventCollection;
	private Cache cache = null;

	public OccationCustomeEventDao() {
		super();
		occCustEventCollection = db.getCollection("occ_cust_event");
		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_OCC_CUST_EVENT_CACHE);
    	CacheBuilder.getInstance().setCaches(cache);
	}

	public String getCustomeEventName(String occasion) {
		List<OccCustEvent> occCustEvents = this.getOccCustEvent();
		if(occCustEvents != null && occCustEvents.size() > 0){
			for(OccCustEvent occCustEvent : occCustEvents){
				if(StringUtils.isNotEmpty(occasion) && occasion.trim().equalsIgnoreCase(occCustEvent.getOccasion())){
					return occCustEvent.getIntCustEvent();
				}
			}
		}
		return StringUtils.EMPTY;
	}

	@SuppressWarnings("unchecked")
	private List<OccCustEvent> getOccCustEvent(){
		String cacheKey = CacheConstant.RTS_OCC_CUST_EVENT_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<OccCustEvent>) element.getObjectValue();
		}else{
		List<OccCustEvent> occCustEvents = new ArrayList<OccCustEvent>();
		DBCursor occCustEventCursor = occCustEventCollection.find();
    	for(DBObject occCustEventObject: occCustEventCursor) {
    		if(occCustEventObject != null){
    			OccCustEvent occCustEvent = new OccCustEvent();
    			occCustEvent.setOccasion(occCustEventObject.get(MongoNameConstants.OCCASION).toString());
    			occCustEvent.setIntCustEvent(occCustEventObject.get(MongoNameConstants.INTERACT_CUSTOME_EVENT).toString());
    			occCustEvents.add(occCustEvent);
    		}
    	}
		if(occCustEvents != null && occCustEvents.size() > 0){
			cache.put(new Element(cacheKey, (List<OccCustEvent>) occCustEvents));
		}
		return occCustEvents;
		}
	}
}
