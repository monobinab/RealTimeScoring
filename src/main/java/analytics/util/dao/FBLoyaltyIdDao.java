package analytics.util.dao;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class FBLoyaltyIdDao {
	//l_id, u
	DB db;
    DBCollection fbLoyaltyCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fbLoyaltyCollection = db.getCollection("fbLoyaltyIds");
    }

	public String getLoyaltyIdFromID(String id){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.FB_ID, id);
		DBCursor cursor = fbLoyaltyCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
		    return obj.get(MongoNameConstants.L_ID).toString();
		}
		return null;
	}
}
