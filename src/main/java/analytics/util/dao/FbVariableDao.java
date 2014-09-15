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

public class FbVariableDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(FbVariableDao.class);
	static DB db;
    DBCollection fbVariableCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public FbVariableDao(){
		fbVariableCollection = db.getCollection("fbVariable");
    }

	public String getVariableFromTopic(String topic){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.FB_KEYWORD, topic);
		DBObject obj = fbVariableCollection.findOne(query);
		if (obj!=null) {
		    return obj.get(MongoNameConstants.FB_VARIABLE).toString();
		}
		return null;
	}
}
