package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnBoostDao {
	DB db;
    DBCollection divLnBoostCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		divLnBoostCollection = db.getCollection("divLnBoost");
    }
    public HashMap<String, List<String>> getDivLnBoost(String sourceTopic){
    	HashMap<String, List<String>> divLnBoostVariblesMap = new HashMap<String, List<String>>();
    	DBCursor divLnBoostVarCursor = divLnBoostCollection.find();
    	for (DBObject divLnBoostDBObject : divLnBoostVarCursor) {

			@SuppressWarnings("unchecked")
			List<HashMap> hashMaps = (List<HashMap>) divLnBoostDBObject
					.get(MongoNameConstants.DLV_VAR);
			List<String> varibles = new ArrayList<String>();

			for (HashMap hashMap : hashMaps) {
				if (hashMap.containsKey(sourceTopic)) {
					varibles.add((String) hashMap.get(sourceTopic));
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
