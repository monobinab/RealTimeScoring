package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberUUIDDao {
	DB db;
    DBCollection memberUuidCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		memberUuidCollection = db.getCollection("memberUUID");
    }
    
    public List<String> getLoyaltyIdsFromUUID(String uuid){
    	List<String> loyaltyIds = new ArrayList<String>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.MUUID_UUID, uuid);
		DBCursor cursor = memberUuidCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
		    loyaltyIds.add(obj.get(MongoNameConstants.L_ID).toString());
		}
		return loyaltyIds;
	}
}
