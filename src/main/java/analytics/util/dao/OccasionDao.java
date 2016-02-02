package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.OccasionInfo;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

public class OccasionDao extends AbstractDao{
	
	private DBCollection occasionInfoCollection;
	private Cache cache = null;
	
	public OccasionDao() {
		super();
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_OCCASIONS_INFO_CACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_OCCASIONS_INFO_CACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
	}
	
	@SuppressWarnings("unchecked")
	public List<OccasionInfo> getOccasionsInfo(){
		String cacheKey = CacheConstant.RTS_OCCASIONS_INFO_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<OccasionInfo>) element.getObjectValue();
		}else{
		List<OccasionInfo> occasionInfos = new ArrayList<OccasionInfo>();
		occasionInfoCollection = db.getCollection("cpsOccasions");
		if(occasionInfoCollection != null){
			DBCursor cursor = occasionInfoCollection.find();
			if(null != cursor){
				cursor.sort(new BasicDBObject("priority", 1));
				if (cursor.size() > 0) {
					while (cursor.hasNext()) {
						DBObject obj = cursor.next();
						if(obj != null){
							OccasionInfo occasionInfo = new OccasionInfo();
							occasionInfo.setOccasion((String)obj.get("occasion"));
							occasionInfo.setPriority((String)obj.get("priority"));
							occasionInfo.setDuration((String)obj.get("duration"));
							occasionInfo.setDaysToCheckInHistory((String)obj.get("daysInHistory"));
							occasionInfo.setIntCustEvent("");
							occasionInfos.add(occasionInfo);
						}
					}
				}
			}
		}
		if(occasionInfos != null && occasionInfos.size() > 0){
			occasionInfoCollection = db.getCollection("occ_cust_event");
			if(occasionInfoCollection != null){
				
				for(OccasionInfo occasionInfo : occasionInfos){
					DBCursor cursor = occasionInfoCollection.find();
					if(cursor != null && cursor.size() > 0 && occasionInfo != null){
						while(cursor.hasNext()) {
							DBObject dbObj = cursor.next();
							if(dbObj != null){
								String occ = (String)dbObj.get("occasion");
								if(StringUtils.isNotEmpty(occ) && occ.equalsIgnoreCase(occasionInfo.getOccasion())){
									occasionInfo.setIntCustEvent((String)dbObj.get("intCustEvent"));
									occasionInfo.setCustEventId((Integer)dbObj.get("custEventId"));
									break;
								}
							}
						}
					}
				}
			}
		}
		if(occasionInfos != null && occasionInfos.size() > 0){
			cache.put(new Element(cacheKey, (List<OccasionInfo>) occasionInfos));
		}
		return occasionInfos;
		}
	}
	
	public OccasionInfo getOccasionInfo(String occ){
		List<OccasionInfo> occasionInfos = this.getOccasionsInfo();
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getOccasion().equalsIgnoreCase(occ)){
					return occasionInfo;
				}
			}
		}
		return null;
	}
	
	public OccasionInfo getOccasionInfoFromPriority(String priority){
		List<OccasionInfo> occasionInfos = this.getOccasionsInfo();
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getPriority().equalsIgnoreCase(priority)){
					return occasionInfo;
				}
			}
		}
		return null;
	} 
}
