package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class SocialVariableDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SocialVariableDao.class);
    DBCollection socialVariable;
    public SocialVariableDao(){
    	super();
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
