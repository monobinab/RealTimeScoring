
package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import com.mongodb.WriteConcernException;

public class MemberBoostsDao extends AbstractDao {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);

	DBCollection memberBoostsCollection;

	public MemberBoostsDao() {
		super();
		memberBoostsCollection = db
				.getCollection(MongoNameConstants.MBR_BOOSTS_COLLECTION);
	}

	public Map<String, Map<String, List<String>>> getMemberBoostsValues(String l_id, Set<String> boostSet) {
		Map<String, Map<String, List<String>>> mapToReturn = new HashMap<String, Map<String, List<String>>>();

		DBObject mbrBoostsDBO = memberBoostsCollection
				.findOne(new BasicDBObject(MongoNameConstants.L_ID, l_id));
		if (mbrBoostsDBO == null) {
			return null;
		}
		
		BasicDBObject mbrBoostList = (BasicDBObject) mbrBoostsDBO.get("boosts");
		if(mbrBoostList == null)
			return null;
		for(String boost:mbrBoostList.keySet()){
			if (!mapToReturn.containsKey(boost)&&boostSet.contains(boost)) {
				mapToReturn.put((String) boost,	new HashMap<String, List<String>>());
				BasicDBObject dateDBList = (BasicDBObject) mbrBoostList.get(boost);
				for (String date : dateDBList.keySet()) {
					if (!mapToReturn.get(boost).containsKey(date)) {
						mapToReturn.get(boost).put(date,new ArrayList<String>());
					}
					BasicDBList valuesDBList = (BasicDBList) dateDBList.get(date);
					for (Object value : valuesDBList) {
						mapToReturn.get(boost).get(date).add(value.toString());
					}
				}
			}
		}
		return mapToReturn;
	}
	
	public Map<String, Map<String, List<String>>> getAllMemberBoostValues(
			String l_id) {

		// Map of boost name to a map of date to a list of values
		Map<String, Map<String, List<String>>> mapToReturn = new HashMap<String, Map<String, List<String>>>();

		DBObject mbrBoostsDBO = memberBoostsCollection
				.findOne(new BasicDBObject(MongoNameConstants.L_ID, l_id));
		if (mbrBoostsDBO == null) {
			return null;
		}
		
		BasicDBObject mbrBoostList = (BasicDBObject) mbrBoostsDBO.get("boosts");
		if(mbrBoostList == null)
			return null;
		for(String boost:mbrBoostList.keySet()){
			if (!mapToReturn.containsKey(boost)) {
				mapToReturn.put((String) boost,	new HashMap<String, List<String>>());
				BasicDBObject dateDBList = (BasicDBObject) mbrBoostList.get(boost);
				for (String date : dateDBList.keySet()) {
					if (!mapToReturn.get(boost).containsKey(date)) {
						mapToReturn.get(boost).put(date,new ArrayList<String>());
					}
					BasicDBList valuesDBList = (BasicDBList) dateDBList.get(date);
					for (Object value : valuesDBList) {
						mapToReturn.get(boost).get(date).add(value.toString());
					}
				}
			}
		}
		return mapToReturn;
	}
	
	

	
	public void writeMemberBoostValues(String l_id, Map<String, Map<String, List<String>>> memberBoostValuesMap) {
		BasicDBObject objectToUpsert = new BasicDBObject(MongoNameConstants.L_ID, l_id);
		BasicDBObject boostDateValues = new BasicDBObject();
		
		
		for(String boost: memberBoostValuesMap.keySet()) {
			for(String date: memberBoostValuesMap.get(boost).keySet()) {
				BasicDBList valuesList = new BasicDBList();
				for(String value: memberBoostValuesMap.get(boost).get(date)) {
					valuesList.add(value);
				}
				boostDateValues.put(boost, new BasicDBObject(date, valuesList));
			}
		}
		
		Map<String, Map<String, List<String>>> previousBoosts = getAllMemberBoostValues(l_id);
		if(previousBoosts != null && !previousBoosts.isEmpty()) {
			for(String boost: previousBoosts.keySet()) {
				for(String date: previousBoosts.get(boost).keySet()) {
					if(memberBoostValuesMap.containsKey(boost) && !memberBoostValuesMap.get(boost).containsKey(date)) {
						BasicDBList valuesList = new BasicDBList();
						for(String value: previousBoosts.get(boost).get(date)) {
							valuesList.add(value);
						}
						((BasicDBObject) boostDateValues.get(boost)).append(date, valuesList);
					} else if(!memberBoostValuesMap.containsKey(boost)) {
						BasicDBList valuesList = new BasicDBList();
						for(String value: previousBoosts.get(boost).get(date)) {
							valuesList.add(value);
						}
						boostDateValues.append(boost, new BasicDBObject());
						((BasicDBObject) boostDateValues.get(boost)).append(date, valuesList);
					}
				}
			}
		}
		
		objectToUpsert.append(MongoNameConstants.BOOSTS_ARRAY, boostDateValues);
		BasicDBObject searchQuery = new BasicDBObject(MongoNameConstants.L_ID, l_id);
		try{
			memberBoostsCollection.update(searchQuery, new BasicDBObject("$set", objectToUpsert), true, false);
		}
		catch(WriteConcernException e){
			LOGGER.error("WriteConcernException occured ", e.getClass(), e.getMessage());
		}
	}
}

