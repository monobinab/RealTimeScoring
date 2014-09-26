
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

public class MemberBoostsDao extends AbstractDao {
	public static void main(String[] args) {
		Map<String, Map<String, List<String>>> memberBoostValuesMap = new HashMap<String, Map<String, List<String>>>();
		String var1 = "BOOST_SYW_LIKE_ALL_APP_TCOUNT";
		String var2 = "BOOST_SYW_LIKE_AU_BATTERY_TCOUNT";
		//String var3 = "BOOST_SYW_LIKE_AU_TIRE_TCOUNT";
		//String var4 = "BOOST_SYW_LIKE_AUTO_TCOUNT";
		String date = "2014-09-23";

		
		memberBoostValuesMap.put(var1, new HashMap<String, List<String>>());
		memberBoostValuesMap.get(var1).put(date,new ArrayList<String>());
		memberBoostValuesMap.get(var1).get(date).add("4");
		memberBoostValuesMap.put(var2, new HashMap<String, List<String>>());
		memberBoostValuesMap.get(var2).put(date,new ArrayList<String>());
		memberBoostValuesMap.get(var2).get(date).add("5");
//		memberBoostValuesMap.put(var3, new HashMap<String, List<String>>());
//		memberBoostValuesMap.get(var3).put(date,new ArrayList<String>());
//		memberBoostValuesMap.get(var3).get(date).add("6");
//		memberBoostValuesMap.put(var4, new HashMap<String, List<String>>());
//		memberBoostValuesMap.get(var4).put(date,new ArrayList<String>());
//		memberBoostValuesMap.get(var4).get(date).add("7");
		
		new MemberBoostsDao().writeMemberBoostValues("Axo0b7SN1eER9shCSj0DX+eSGag=", memberBoostValuesMap);
	}

	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberVariablesDao.class);

	DBCollection memberBoostsCollection;

	public MemberBoostsDao() {
		super();
		memberBoostsCollection = db
				.getCollection(MongoNameConstants.MBR_BOOSTS_COLLECTION);
	}

	public Map<String, Map<String, List<String>>> getMemberBoostsValues(
			String l_id, Set<String> boostSet) {

		// Map of boost name to a map of date to a list of values
		Map<String, Map<String, List<String>>> mapToReturn = new HashMap<String, Map<String, List<String>>>();

		DBObject mbrBoostsDBO = memberBoostsCollection
				.findOne(new BasicDBObject(MongoNameConstants.L_ID, l_id));
		if (mbrBoostsDBO == null) {
			return null;
		}
		BasicDBList mbrBoostsList = (BasicDBList) mbrBoostsDBO
				.get(MongoNameConstants.BOOSTS_ARRAY);
		if (mbrBoostsList == null) {
			return null;
		}

		for (Object boostObj : mbrBoostsList) {
			for (String boost : ((DBObject) boostObj).keySet()) {
				if (!mapToReturn.containsKey(boost) && boostSet.contains(boost)) {
					mapToReturn.put((String) boost,	new HashMap<String, List<String>>());
					BasicDBList dateDBList = (BasicDBList) ((DBObject) boostObj)
							.get(boost);
					for (Object dateValue : dateDBList) {
						for (String date : ((DBObject) dateValue).keySet()) {
							if (!mapToReturn.get(boost).containsKey(date)) {
								mapToReturn.get(boost).put((String) date,new ArrayList<String>());
							}
							BasicDBList valuesDBList = (BasicDBList) ((DBObject)dateValue).get(date);
							for (Object value : valuesDBList) {
								mapToReturn.get(boost).get(date).add(value.toString());
							}
						}
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
		BasicDBList mbrBoostsList = (BasicDBList) mbrBoostsDBO
				.get(MongoNameConstants.BOOSTS_ARRAY);
		if (mbrBoostsList == null) {
			return null;
		}

		for (Object boostObj : mbrBoostsList) {
			for (String boost : ((DBObject) boostObj).keySet()) {
				if (!mapToReturn.containsKey(boost)) {
					mapToReturn.put((String) boost,	new HashMap<String, List<String>>());
					BasicDBList dateDBList = (BasicDBList) ((DBObject) boostObj)
							.get(boost);
					for (Object dateValue : dateDBList) {
						for (String date : ((DBObject) dateValue).keySet()) {
							if (!mapToReturn.get(boost).containsKey(date)) {
								mapToReturn.get(boost).put((String) date,new ArrayList<String>());
							}
							BasicDBList valuesDBList = (BasicDBList) ((DBObject)dateValue).get(date);
							for (Object value : valuesDBList) {
								mapToReturn.get(boost).get(date).add(value.toString());
							}
						}
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
				for(String date: memberBoostValuesMap.get(boost).keySet()) {
					if(memberBoostValuesMap.containsKey(boost) && !memberBoostValuesMap.get(boost).containsKey(date)) {
						BasicDBList valuesList = new BasicDBList();
						for(String value: previousBoosts.get(boost).get(date)) {
							valuesList.add(value);
						}
						boostDateValues.put(boost, new BasicDBObject(date, valuesList));
					} else if(!memberBoostValuesMap.containsKey(boost)) {
						BasicDBList valuesList = new BasicDBList();
						for(String value: previousBoosts.get(boost).get(date)) {
							valuesList.add(value);
						}
						boostDateValues.put(boost, new BasicDBObject(date, valuesList));
					}
				}
			}
		}
		
		objectToUpsert.append(MongoNameConstants.BOOSTS_ARRAY, boostDateValues);
		BasicDBObject searchQuery = new BasicDBObject(MongoNameConstants.L_ID, l_id);
		memberBoostsCollection.update(searchQuery, new BasicDBObject("$set", objectToUpsert), true, false);
	}
}

