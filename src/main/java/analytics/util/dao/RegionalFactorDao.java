package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RegionalFactorDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(RegionalFactorDao.class);
	DBCollection regionalFactorColl;
	private Map<String, Double> regionalFactorMap;

	public Map<String, Double> getRegionalFactorMap() {
		return regionalFactorMap;
	}

	public void setRegionalFactorMap(Map<String, Double> regionalFactorMap) {
		this.regionalFactorMap = regionalFactorMap;
	}

	public RegionalFactorDao() {
		super();
		regionalFactorColl = db.getCollection("regionalAdjustmentFactors"); 
		}

	public  void populateRegionalFactors(){
		DBCursor cursor = regionalFactorColl.find();
		regionalFactorMap = new HashMap<String, Double>();
		while(cursor.hasNext()){
			DBObject dbObj = cursor.next();
			if(dbObj != null){
				String key = (String) dbObj.get(MongoNameConstants.MODEL_ID) + (String)dbObj.get(MongoNameConstants.REGIONAL_STATE);
				regionalFactorMap.put(key, (Double) dbObj.get(MongoNameConstants.FACTOR));
			}
		}
			setRegionalFactorMap(regionalFactorMap);
	}
}