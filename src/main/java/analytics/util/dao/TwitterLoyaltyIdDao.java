package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class TwitterLoyaltyIdDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TwitterLoyaltyIdDao.class);
    DBCollection twLoyaltyCollection;
    public TwitterLoyaltyIdDao(){
    	super();
		twLoyaltyCollection = db.getCollection("twLoyaltyIds");
    }

	public String getLoyaltyIdFromID(String id){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.SOCIAL_ID, id);
		DBCursor cursor = twLoyaltyCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
		    return obj.get(MongoNameConstants.L_ID).toString();
		}
		return null;
	}
}