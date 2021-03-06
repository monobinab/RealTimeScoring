package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import analytics.util.MongoNameConstants;
import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheConstant;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.objects.DC;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DcAidVarStrengthDao extends AbstractDao{

	private DBCollection dcAidVarStrength;
	private Cache cache = null;
	
	public DcAidVarStrengthDao() {
		super();
		dcAidVarStrength = db.getCollection("dcAidVariableStrength"); // MongoNameConstants.PID_DIV_LN_COLLECTION
		cache = CacheManager.getInstance().getCache(CacheConstant.RTS_CACHE_DIV_AID_VAR_STRENGTH_CACHE);
    	if(null == cache){
			cache = CacheManager.newInstance().getCache(CacheConstant.RTS_CACHE_DIV_AID_VAR_STRENGTH_CACHE);
	    	CacheBuilder.getInstance().setCaches(cache);
    	}
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Map<String, Integer>> getdcAidVarStrenghtMap(){
		String cacheKey = CacheConstant.RTS_DIV_AID_VAR_STRENGTH_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, Map<String, Integer>>) element.getObjectValue();
		}else{
		DBCursor modelsCursor = dcAidVarStrength.find();
		Map<String, Map<String, Integer>> dcAidVarStrengthMap = new HashMap<String, Map<String, Integer>>();
		for(DBObject dbObj : modelsCursor){
			Map<String, Integer> varStrengthMap = new HashMap<String, Integer>();
			String aid = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_AID);
			String var = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_MODEL);
			Integer strength = (Integer) dbObj.get(MongoNameConstants.DC_AID_VAR_SCORE);
			varStrengthMap.put(var, strength);
			if(!dcAidVarStrengthMap.containsKey(aid)){
				dcAidVarStrengthMap.put(aid, varStrengthMap);
			}
			else{
				dcAidVarStrengthMap.get(aid).put(var, strength);
			}
		}
		if(dcAidVarStrengthMap != null && dcAidVarStrengthMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, Map<String, Integer>>) dcAidVarStrengthMap));
		}
		return dcAidVarStrengthMap;
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public Map<String, DC> getdcAidVarStrenghtTags(){
		String cacheKey = CacheConstant.RTS_DIV_AID_VAR_STRENGTH_CACHE_KEY;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<String, DC>) element.getObjectValue();
		}else{
		DBCursor modelsCursor = dcAidVarStrength.find();
		Map<String, DC> dcAidVarStrengthMap = new HashMap<String, DC>();
		for(DBObject dbObj : modelsCursor){
			Map<String, Integer> varStrengthMap = new HashMap<String, Integer>();
			String aid = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_AID);
			String var = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_MODEL);
			Integer strength = (Integer) dbObj.get(MongoNameConstants.DC_AID_VAR_SCORE);
			List<String> tags = new ArrayList<String>();
			if(dbObj.containsField("tags")){
				tags = (List<String>) dbObj.get("tags");
			}
			varStrengthMap.put(var, strength);
			DC dcDto = new DC();
			dcDto.setBoostStrengthMap(varStrengthMap);
			dcDto.setTags(tags);
			if(!dcAidVarStrengthMap.containsKey(aid)){
				dcAidVarStrengthMap.put(aid, dcDto);
			}
			else{
				dcAidVarStrengthMap.get(aid).getBoostStrengthMap().put(var, strength);
				dcAidVarStrengthMap.get(aid).getTags().addAll(tags);
			}
		}
		if(dcAidVarStrengthMap != null && dcAidVarStrengthMap.size() > 0){
			cache.put(new Element(cacheKey, (Map<String, DC>) dcAidVarStrengthMap));
		}
		return dcAidVarStrengthMap;
		}
	}
}