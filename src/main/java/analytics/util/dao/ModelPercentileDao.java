package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import com.mongodb.DBCollection;

public class ModelPercentileDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ModelPercentileDao.class);
    DBCollection modelPercentileCollection;
    
    public ModelPercentileDao() {
    	super();
    	modelPercentileCollection = db.getCollection("modelPercentile");
    }
	public Map<Integer, Map<Integer, Double>> getModelPercentiles(){
		return new HashMap<Integer, Map<Integer,Double>>();
	}
	
}
