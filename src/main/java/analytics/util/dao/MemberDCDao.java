package analytics.util.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.mongodb.util.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberDCDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(MemberTraitsDao.class);
	DBCollection memberDCCollection;

	public MemberDCDao() {
		super();
		memberDCCollection = db.getCollection("memberDC");
	}

	public void addDateDC(String l_id, String obj_str) {
		// System.out.println(obj_str);
		DBObject query = new BasicDBObject();
		DBObject obj = (DBObject) JSON.parse(obj_str);
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject object = memberDCCollection.findOne(query);
		if (object == null) {
			BasicDBList dateDCList = new BasicDBList();
			dateDCList.add(obj);
			object = new BasicDBObject();
			object.put(MongoNameConstants.L_ID, l_id);
			object.put(MongoNameConstants.MT_DATES_ARR, dateDCList);
			memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
		} else {
			BasicDBList dateDCList = (BasicDBList) object.get(MongoNameConstants.MT_DATES_ARR);
			DBObject dc_obj = (DBObject) dateDCList.get(dateDCList.size() - 1);
			if (dc_obj.get("d").equals(obj.get("d"))) {
				DBObject dcMap = (BasicDBObject) dc_obj.get("dc");
				DBObject objMap = (DBObject) obj.get("dc");
				Set<String> keyset = objMap.keySet();
				String key = keyset.iterator().next();
				// type needs to be handled here
				Object originVal = dcMap.get(key);
				Object currVal = objMap.get(key);
				Double originDoub = JsonUtils.convertToDouble(originVal);
				Double currDoub = JsonUtils.convertToDouble(currVal);
				dcMap.put(key, originDoub + currDoub);
			} else {
				dateDCList.add(obj);
			}

			memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
		}

	}

	// return a map
	public Map<String, String> getDateStrengthMap(String category, String l_id) {
		Map<String, String> map = new HashMap<String, String>();
		DBObject query = new BasicDBObject();
		query.put("l_id", l_id);
		DBObject object = memberDCCollection.findOne(query);
		if (object != null) {
			// TODO: filter by category
			BasicDBList list = (BasicDBList) object.get("date");
			for (int i = 0; i < list.size(); i++) {
				DBObject dateObject = (DBObject) list.get(i);
				String date = (String) dateObject.get("d");
				DBObject dc = (DBObject) dateObject.get("dc");
				DBObject dc_filtered_by_category = new BasicDBObject();
				//dc_filtered_by_category.put(category, dc.get(category));
				map.put(date, dc.get(category).toString());
			}
		}
		return map;
	}

}
