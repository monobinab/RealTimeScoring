package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnBoostDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnBoostDao.class);
	static DB db;
    DBCollection divLnBoostCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public DivLnBoostDao(){
		divLnBoostCollection = db.getCollection("divLnBoost");
    }
    public HashMap<String, List<String>> getDivLnBoost(String sourceTopic){
    	HashMap<String, List<String>> divLnBoostVariblesMap = new HashMap<String, List<String>>();
    	DBCursor divLnBoostVarCursor = divLnBoostCollection.find();
    	for (DBObject divLnBoostDBObject : divLnBoostVarCursor) {
			List<HashMap<String,String>> hashMaps = (List<HashMap<String,String>>) divLnBoostDBObject
					.get(MongoNameConstants.DLV_VAR);
			List<String> varibles = new ArrayList<String>();

			for (HashMap<String,String> hashMap : hashMaps) {
				if (hashMap.containsKey(sourceTopic)) {
					varibles.add(hashMap.get(sourceTopic));
				}
			}
			if (varibles.size() > 0
					&& !(divLnBoostVariblesMap.containsKey(divLnBoostDBObject
							.get(MongoNameConstants.DLV_DIV)))) {

				divLnBoostVariblesMap.put((String) divLnBoostDBObject.get(MongoNameConstants.DLV_DIV),
						varibles);
			} else if (varibles.size() > 0) {
				List<String> variblevalues = divLnBoostVariblesMap
						.get(divLnBoostDBObject.get(MongoNameConstants.DLV_DIV));
				varibles.addAll(variblevalues);
				divLnBoostVariblesMap.put((String) divLnBoostDBObject.get(MongoNameConstants.DLV_DIV),
						varibles);
			}
    	}

    	return divLnBoostVariblesMap;
    }
}
