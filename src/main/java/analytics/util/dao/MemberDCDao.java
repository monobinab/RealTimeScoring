package analytics.util.dao;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.mongodb.util.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.JsonUtils;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
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
		DBObject query = new BasicDBObject();
		DBObject obj = (DBObject) JSON.parse(obj_str);
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject object = memberDCCollection.findOne(query);
		if (object == null) {
			BasicDBList dateDCList = new BasicDBList();
			BasicDBObject dcObject = (BasicDBObject) obj.get("dc");
			Set<String> keyset = dcObject.keySet();
			Iterator<String> it = keyset.iterator();
			while (it.hasNext()) {
				String key = it.next();
				dcObject.put(key, JsonUtils.convertToDouble(dcObject.get(key)));
			}
			dateDCList.add(obj);
			object = new BasicDBObject();
			object.put(MongoNameConstants.L_ID, l_id);
			object.put(MongoNameConstants.MT_DATES_ARR, dateDCList);
			memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
		} else {
			BasicDBList dateDCList = (BasicDBList) object.get(MongoNameConstants.MT_DATES_ARR);
			if (dateDCList != null && dateDCList.size() > 0) {
				DBObject dc_obj = (DBObject) dateDCList.get(dateDCList.size() - 1);
				if (dc_obj.get("d").equals(obj.get("d"))) {
					DBObject dcMap = (BasicDBObject) dc_obj.get("dc");
					DBObject objMap = (DBObject) obj.get("dc");
					Set<String> keyset = objMap.keySet();
					String key = keyset.iterator().next();
					Object originVal = dcMap.get(key);
					Object currVal = objMap.get(key);
					if (originVal == null) {
						originVal = 0.0;
					}
					Double originDoub = JsonUtils.convertToDouble(originVal);
					Double currDoub = JsonUtils.convertToDouble(currVal);
					dcMap.put(key, originDoub + currDoub);
				} else {
					// TODO: Eliminate duplicated code
					BasicDBObject dcObject = (BasicDBObject) obj.get("dc");
					Set<String> keyset = dcObject.keySet();
					Iterator<String> it = keyset.iterator();
					while (it.hasNext()) {
						String key = it.next();
						dcObject.put(key, JsonUtils.convertToDouble(dcObject.get(key)));
					}
					dateDCList.add(obj);
				}
			} else if (dateDCList == null || dateDCList.size() == 0) {
				// TODO: Eliminate duplicated code
				BasicDBObject dcObject = (BasicDBObject) obj.get("dc");
				Set<String> keyset = dcObject.keySet();
				Iterator<String> it = keyset.iterator();
				while (it.hasNext()) {
					String key = it.next();
					dcObject.put(key, JsonUtils.convertToDouble(dcObject.get(key)));
				}
				if(dateDCList != null){
					dateDCList.add(obj);
				}
				else{
					dateDCList = new BasicDBList();
					dateDCList.add(obj);
					object.put(MongoNameConstants.MT_DATES_ARR, dateDCList);
				}
					
			}
			memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), new BasicDBObject("$set", object), true, false);
		}

	}

	// return a map
	public Map<String, String> getDateStrengthMap(String category, String l_id) {
		Map<String, String> map = new HashMap<String, String>();
		DBObject query = new BasicDBObject();
		query.put("l_id", l_id);
		DBObject object = memberDCCollection.findOne(query);
		if (object != null) {
			BasicDBList list = (BasicDBList) object.get("date");
			if (list != null) {
				for (int i = 0; i < list.size(); i++) {
					DBObject dateObject = (DBObject) list.get(i);
					String date = (String) dateObject.get("d");
					DBObject dc = (DBObject) dateObject.get("dc");
					// TODO: category here will be changed to an array of
					// category because one var can be affected by multiple
					// categories
					// DBObject dc_filtered_by_category = new BasicDBObject();
					// dc_filtered_by_category.put(category, dc.get(category));
					Object val = dc.get(category);
					if (val == null) {
						val = 0.0;
					}
					map.put(date, val.toString());
				}
			}
		}
		return map;
	}

	public void setDB(DB db) {
		this.db = db;
		memberDCCollection = db.getCollection("memberDC");
	}

}
