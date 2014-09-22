package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberBoostsDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);
	
    DBCollection memberBoostsCollection;
    
    public MemberBoostsDao(){
    	super();
    	memberBoostsCollection = db.getCollection(MongoNameConstants.MBR_BOOSTS_COLLECTION);
    }

	public Map<String, Map<String, List<String>>> getMemberBoostsValues(
			String l_id, Set<String> boostSet) {
		
		// Map of boost name to a map of date to a list of values
		Map<String, Map<String, List<String>>> mapToReturn = new HashMap<String, Map<String, List<String>>>();
		
		DBObject mbrBoostsDBO = memberBoostsCollection.findOne(new BasicDBObject(MongoNameConstants.L_ID, l_id));
		if(mbrBoostsDBO == null) {
			return null;
		}
		BasicDBList mbrBoostsList = (BasicDBList) mbrBoostsDBO.get(MongoNameConstants.BOOSTS_ARRAY);
		if(mbrBoostsList == null) {
			return null;
		}
		
		for(String boost: mbrBoostsList.keySet()) {
			if(!mapToReturn.containsKey(boost) && boostSet.contains(boost)) {
				mapToReturn.put(boost, new HashMap<String, List<String>>());
				BasicDBList dateDBList = (BasicDBList) mbrBoostsList.get(boost);
				for(String date: dateDBList.keySet()) {
					if(!mapToReturn.get(boost).containsKey(date)) {
						mapToReturn.get(boost).put(date, new ArrayList<String>());
					}
					BasicDBList valuesDBList = (BasicDBList) dateDBList.get(date);
					for(String value: valuesDBList.keySet()) {
						mapToReturn.get(boost).get(date).add(value);
					}
				}
			}
		}
		return mapToReturn;
	}
}
