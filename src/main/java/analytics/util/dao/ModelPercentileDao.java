package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;







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
}
