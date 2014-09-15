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

public class MemberZipDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(MemberZipDao.class);
	static DB db;
    DBCollection memberZipCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public MemberZipDao(){
		memberZipCollection = db.getCollection("memberZip");
    }
	public String getMemberZip(String l_id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBCursor cursor = memberZipCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
		    return obj.get(MongoNameConstants.ZIP).toString();
		}
		return null;
	}

}
