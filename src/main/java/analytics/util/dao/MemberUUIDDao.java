package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberUUIDDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(MemberUUIDDao.class);
	static DB db;
    DBCollection memberUuidCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public MemberUUIDDao(){
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
