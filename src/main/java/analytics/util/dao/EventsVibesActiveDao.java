package analytics.util.dao;

import java.util.HashMap;

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

public class EventsVibesActiveDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(EventsVibesActiveDao.class);
	private DBCollection eventsVibesActiveCollection;
	private Cache cache = null;

	
	public EventsVibesActiveDao() {
		super();
		eventsVibesActiveCollection = db.getCollection("eventsVibesActive");
		LOGGER.info("collection in EventsVibesActiveDao: " + eventsVibesActiveCollection.getFullName());
		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_EVENTS_VIBES_ACTIVE_CACHE);
    	CacheBuilder.getInstance().setCaches(cache);
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, HashMap<String, String>> getVibesActiveEventsList(){
		String cacheKey = CacheConstant.RTS_EVENTS_VIBES_ACTIVE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<String, HashMap<String, String>>) element.getObjectValue();
		}else{
		DBCursor dbCursor = eventsVibesActiveCollection.find();
		DBObject record = null;
		HashMap<String, HashMap<String, String>> activeEventsMap = new HashMap<String, HashMap<String, String>>();
		while (dbCursor.hasNext()) {
			record = dbCursor.next();
			if(record!=null){
				
				if(activeEventsMap.get((String)record.get(MongoNameConstants.PURCHASE_OCCASSION))!=null){
					HashMap<String, String> map = activeEventsMap.get((String)record.get(MongoNameConstants.PURCHASE_OCCASSION));
					map.put((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT), (String)record.get(MongoNameConstants.CUST_VIBES_EVENT));
				}
				else{	
					HashMap<String, String> map = new HashMap<String, String>(); 
					map.put((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT), (String)record.get(MongoNameConstants.CUST_VIBES_EVENT));
					activeEventsMap.put((String)record.get(MongoNameConstants.PURCHASE_OCCASSION), map);
				}
			}
		}
		if(activeEventsMap != null && activeEventsMap.size() > 0){
			cache.put(new Element(cacheKey, (HashMap<String, HashMap<String, String>>) activeEventsMap));
		}
		return activeEventsMap;
		}
	}
}

