package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class FBLoyaltyIdDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(FBLoyaltyIdDao.class);
	static DB db;
    DBCollection fbLoyaltyCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public FBLoyaltyIdDao(){
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
