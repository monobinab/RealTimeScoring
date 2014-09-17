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

public class SocialVariableDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(SocialVariableDao.class);
	static DB db;
    DBCollection socialVariable;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public SocialVariableDao(){
		socialVariable = db.getCollection("socialVariable");
    }

	public String getVariableFromTopic(String topic){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.SOCIAL_KEYWORD, topic);
		DBObject obj = socialVariable.findOne(query);
		if (obj!=null) {
		    return obj.get(MongoNameConstants.SOCIAL_VARIABLE).toString();
		}
		return null;
	}
}
