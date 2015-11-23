package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.TagsResponsesActive;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagResponsysActiveDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(TagResponsysActiveDao.class);
	private DBCollection tagResponsysActiveCollection;
	private Cache cache = null;

	/**
	 * t,b,s,po
	 */
	public TagResponsysActiveDao() {
		super();
		tagResponsysActiveCollection = db.getCollection("tagsResponsysActive");
		LOGGER.info("colelction in tagMetadataDao: " + tagResponsysActiveCollection.getFullName());
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_TAGSRESPONSESACTIVECACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_TAGSRESPONSESACTIVECACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
	}

	public HashSet<String> getResponsysActiveTagsList(){
		HashSet<String> activeTagsList = new HashSet<String>();
		List<TagsResponsesActive> tagsResponsesActives = this.getTagsResponsesActive();
		if(tagsResponsesActives != null && tagsResponsesActives.size() > 0){
			for(TagsResponsesActive tagsResponsesActive : tagsResponsesActives){
				activeTagsList.add(tagsResponsesActive.getBu());
			}
		}
		return activeTagsList;
	}
	
	public List<String> getActiveResponsysTagsList(){
		List<String> activeTags = new ArrayList<String>();
		List<TagsResponsesActive> tagsResponsesActives = this.getTagsResponsesActive();
		if(tagsResponsesActives != null && tagsResponsesActives.size() > 0){
			for(TagsResponsesActive tagsResponsesActive : tagsResponsesActives){
				if(tagsResponsesActive.getOcc().equalsIgnoreCase("Unknown")){
				activeTags.add(tagsResponsesActive.getBu());
				}
			}
		}
		return activeTags;
	}
	
	@SuppressWarnings("unchecked")
	private List<TagsResponsesActive> getTagsResponsesActive(){
		String cacheKey = CacheConstant.RTS_TAGSRESPONSESACTIVE_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (List<TagsResponsesActive>) element.getObjectValue();
		}else{
		 List<TagsResponsesActive> tagsResponsesActives = new ArrayList<TagsResponsesActive>();
		 DBCursor dbCursor = tagResponsysActiveCollection.find();
		 if(dbCursor != null && dbCursor.count() > 0){
			 while(dbCursor.hasNext()){
				DBObject dbObj = dbCursor.next();
				if(dbObj != null){
					TagsResponsesActive tagsResponsesActive = new TagsResponsesActive();
					tagsResponsesActive.setBu((String) dbObj.get("BU"));
					tagsResponsesActive.setOcc((String) dbObj.get("OCC"));
					tagsResponsesActives.add(tagsResponsesActive);
				}
			 }
		 }
		 if(tagsResponsesActives != null && tagsResponsesActives.size() > 0){
				cache.put(new Element(cacheKey, (List<TagsResponsesActive>) tagsResponsesActives));
		 }
		 return tagsResponsesActives;
		}
	}
}

