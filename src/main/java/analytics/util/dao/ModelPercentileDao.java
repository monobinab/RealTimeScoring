package analytics.util.dao;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class ModelPercentileDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ModelPercentileDao.class);
    DBCollection modelPercentileCollection;
    
    public ModelPercentileDao() {
    	super();
    	modelPercentileCollection = db.getCollection("modelPercentile");
    }
	public Map<Integer, Map<Integer, Double>> getModelPercentiles(){
		Map<Integer, Map<Integer, Double>> modelPercentilesMap = new HashMap<Integer, Map<Integer,Double>>();
		DBCursor modelPercentileCursor = modelPercentileCollection.find();
		for(DBObject modelPercentileEntry:modelPercentileCursor){
			Integer modelId = Integer.parseInt((String) modelPercentileEntry.get("modelId"));
			if(!modelPercentilesMap.containsKey(modelId)){
				modelPercentilesMap.put(modelId, new HashMap<Integer, Double>());
			}
			modelPercentilesMap.get(modelId).put(Integer.parseInt((String)modelPercentileEntry.get("percentile")), Double.parseDouble((String)modelPercentileEntry.get("maxScore")));
			
		}
		return modelPercentilesMap;
	}
	
	public String getSingleModelPercentile(String modelId, String percentile){
		
		List<BasicDBObject> query = new ArrayList<BasicDBObject>();
		query.add(new BasicDBObject(MongoNameConstants.MODEL_ID, modelId));
		query.add(new BasicDBObject(MongoNameConstants.MODEL_PERC, percentile));
		BasicDBObject andQuery = new BasicDBObject();
		andQuery.put("$and", query);
		DBObject dbObj = modelPercentileCollection.findOne(andQuery);
		if(dbObj != null){
			return dbObj.get(MongoNameConstants.MAX_SCORE).toString();
		}
		else{
			LOGGER.info("No maxscore found for model " + modelId + " with "+percentile+"%");
			return null;
		}
	}
	
	public HashMap<String,String> getModelWith98Percentile(){
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
		return modelPercentile;
	}
	
	public HashMap<Integer, TreeMap<Integer, Double>> getModelScorePercentilesMap(){
		HashMap<Integer, TreeMap<Integer, Double>> modelPercentilesMap = new HashMap<Integer, TreeMap<Integer,Double>>();
		DBCursor modelPercentileCursor = modelPercentileCollection.find();
		for(DBObject modelPercentileEntry:modelPercentileCursor){
			Integer modelId = Integer.parseInt((String) modelPercentileEntry.get("modelId"));
			if(!modelPercentilesMap.containsKey(modelId)){
				modelPercentilesMap.put(modelId, new TreeMap<Integer, Double>(Collections.reverseOrder()));
			}
			modelPercentilesMap.get(modelId).put(Integer.parseInt((String)modelPercentileEntry.get("percentile")), Double.parseDouble((String)modelPercentileEntry.get("maxScore")));
			
		}
		return modelPercentilesMap;
	}
	
	/*
	public static void main(String[] args){
		ModelPercentileDao dao = new ModelPercentileDao();
		dao.getSingleModelPercentile("68");
	}*/
}