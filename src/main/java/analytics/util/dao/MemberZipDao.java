package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberZipDao extends AbstractDao{
	static final Logger LOGGER = LoggerFactory
			.getLogger(MemberZipDao.class);
    DBCollection memberZipCollection;
    public MemberZipDao(){
    	super();
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
