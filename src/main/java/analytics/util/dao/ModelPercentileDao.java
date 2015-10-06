package analytics.util.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.caching.CacheBuilder;
import analytics.util.dao.caching.CacheWrapper;
import analytics.util.dao.caching.RTSCacheConstant;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelPercentileDao extends AbstractDao{

	private static final Logger LOGGER = LoggerFactory.getLogger(ModelPercentileDao.class);
    
	private DBCollection modelPercentileCollection;
    
	private Cache cache = null;
    
    public ModelPercentileDao() {
    	super();
    	modelPercentileCollection = db.getCollection("modelPercentile");
    	cache = CacheManager.newInstance().getCache(RTSCacheConstant.RTS_CACHE_MODELPERCENTILECACHE);
    	CacheBuilder.getInstance().setCaches(cache);
    }
    
	@SuppressWarnings("unchecked")
	public Map<Integer, Map<Integer, Double>> getModelPercentiles(){
		String cacheKey = "getModelPercentiles";
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (Map<Integer, Map<Integer, Double>>) element.getObjectValue();
		}else{
			Map<Integer, Map<Integer, Double>> modelPercentilesMap = new HashMap<Integer, Map<Integer,Double>>();
			DBCursor modelPercentileCursor = modelPercentileCollection.find();
			for(DBObject modelPercentileEntry:modelPercentileCursor){
				Integer modelId = Integer.parseInt((String) modelPercentileEntry.get("modelId"));
				if(!modelPercentilesMap.containsKey(modelId)){
					modelPercentilesMap.put(modelId, new HashMap<Integer, Double>());
				}
				modelPercentilesMap.get(modelId).put(Integer.parseInt((String)modelPercentileEntry.get("percentile")), Double.parseDouble((String)modelPercentileEntry.get("maxScore")));
				
			}
			if(modelPercentilesMap != null && modelPercentilesMap.size() > 0){
				cache.put(new Element(cacheKey, (Map<Integer, Map<Integer, Double>>) modelPercentilesMap));
			}
			return modelPercentilesMap;
		}
	}
	
	public String getSingleModelPercentile(String modelId, String percentile){
		String cacheKey = modelId + percentile;
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (String) element.getObjectValue();
		}else{
			List<BasicDBObject> query = new ArrayList<BasicDBObject>();
			query.add(new BasicDBObject(MongoNameConstants.MODEL_ID, modelId));
			query.add(new BasicDBObject(MongoNameConstants.MODEL_PERC, percentile));
			BasicDBObject andQuery = new BasicDBObject();
			andQuery.put("$and", query);
			DBObject dbObj = modelPercentileCollection.findOne(andQuery);
			if(dbObj != null){
				String singleModelPerc = dbObj.get(MongoNameConstants.MAX_SCORE).toString();
				if(StringUtils.isNotEmpty(singleModelPerc)){
					cache.put(new Element(cacheKey, singleModelPerc));
				}
				return singleModelPerc;
			}
			else{
				LOGGER.info("No maxscore found for model " + modelId + " with "+percentile+"%");
				return null;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public HashMap<String,String> getModelWith98Percentile(){
		String cacheKey = "getModelWith98Percentile";
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<String,String>) element.getObjectValue();
		}else{
			HashMap <String,String> modelPercentile = new HashMap<String, String>();
			BasicDBObject query = new BasicDBObject();
			query.put(MongoNameConstants.MODEL_PERC, "98");
			
			DBCursor modelScoreCursor = modelPercentileCollection.find(query);
			while(modelScoreCursor.hasNext()){
				DBObject modelScore = modelScoreCursor.next();
				if(modelScore!=null)
				{
					modelPercentile.put(modelScore.get(MongoNameConstants.MODEL_ID).toString(), modelScore.get(MongoNameConstants.MAX_SCORE).toString());
				}
			}
			if(modelPercentile != null && modelPercentile.size() > 0){
				cache.put(new Element(cacheKey, modelPercentile));
			}
			return modelPercentile;
		}
	}
	
	@SuppressWarnings("unchecked")
	public HashMap<Integer, TreeMap<Integer, Double>> getModelScorePercentilesMap(){
		String cacheKey = "getModelScorePercentilesMap";
		Element element = CacheWrapper.getInstance().isCacheKeyExist(cache, cacheKey);
		if(element != null && element.getObjectKey().equals(cacheKey)){
			return (HashMap<Integer, TreeMap<Integer, Double>>) element.getObjectValue();
		}else{
			HashMap<Integer, TreeMap<Integer, Double>> modelPercentilesMap = new HashMap<Integer, TreeMap<Integer,Double>>();
			DBCursor modelPercentileCursor = modelPercentileCollection.find();
			for(DBObject modelPercentileEntry:modelPercentileCursor){
				Integer modelId = Integer.parseInt((String) modelPercentileEntry.get("modelId"));
				if(!modelPercentilesMap.containsKey(modelId)){
					modelPercentilesMap.put(modelId, new TreeMap<Integer, Double>(Collections.reverseOrder()));
				}
				modelPercentilesMap.get(modelId).put(Integer.parseInt((String)modelPercentileEntry.get("percentile")), Double.parseDouble((String)modelPercentileEntry.get("maxScore")));
				
			}
			if(modelPercentilesMap != null && modelPercentilesMap.size() > 0){
				cache.put(new Element(cacheKey, modelPercentilesMap));
			}
		return modelPercentilesMap;
		}
	}
	
	/*
	public static void main(String[] args){
		ModelPercentileDao dao = new ModelPercentileDao();
		dao.getSingleModelPercentile("68");
	}*/
}
