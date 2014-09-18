package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberUUIDDao extends AbstractDao{
	static final Logger LOGGER = LoggerFactory
			.getLogger(MemberUUIDDao.class);
    DBCollection memberUuidCollection;
    public MemberUUIDDao(){
    	super();
		memberUuidCollection = db.getCollection("memberUUID");
    }
    
    public List<String> getLoyaltyIdsFromUUID(String uuid){
    	List<String> loyaltyIds = new ArrayList<String>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.MUUID_UUID, uuid);
		DBCursor cursor = memberUuidCollection.find(query);
		while (cursor.hasNext()) {
			DBObject obj = cursor.next();
		    loyaltyIds.add(obj.get(MongoNameConstants.L_ID).toString());
		}
		return loyaltyIds;
	}
}
