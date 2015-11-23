package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.TagMetadata;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class DivLineBuSubDao extends AbstractDao {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DivLineBuSubDao.class);
	private DBCollection divLineBuSubCollection;
	private Cache cache = null;

	/**
	 * t,b,s,po
	 */
	public DivLineBuSubDao() {
		super();
		divLineBuSubCollection = db.getCollection("divLnBuName");
		LOGGER.info("collection in tagMetadataDao: " + divLineBuSubCollection.getFullName());
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_DIV_LN_BUNAMECACHE);
    	if(null == cache){
    		cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIV_LN_BUNAMECACHE);
    		CacheBuilder.getInstance().setCaches(cache);
    	}
	}


	public TagMetadata getBuSubBu(TagMetadata tagMetadata,String divLine){
		String cacheKey = CacheConstant.RTS_DIV_LN_BU_NAME_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (TagMetadata) element.getObjectValue();
		}else{
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.DLBS_DIV, divLine);
		
		DBObject dbObj = divLineBuSubCollection.findOne(query);
		
		if(dbObj != null){
			tagMetadata.setDivLine(tagMetadata.getDivLine()!=null ? tagMetadata.getDivLine()+"," +divLine: divLine);
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_BU) : (String)dbObj.get(MongoNameConstants.DLBS_BU));
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_SUB) : (String)dbObj.get(MongoNameConstants.DLBS_SUB));
	
		}/*else{
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+",null" : "null");
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+",null" : "null");
		}*/
		if(tagMetadata != null){
			cache.put(new Element(cacheKey, (TagMetadata) tagMetadata));
		}
		return tagMetadata;
		}
	}
}
