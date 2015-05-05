package analytics.util.dao;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OccationCustomeEventDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OccationCustomeEventDao.class);
	DBCollection occCustEventCollection;

	public OccationCustomeEventDao() {
		super();
		occCustEventCollection = db.getCollection("occ_cust_event");
	}

	public String getCustomeEventName(String occasion) {
		BasicDBObject query = new BasicDBObject();
		//query.put(MongoNameConstants.OCCASION, occasion);
		
		query.put(MongoNameConstants.OCCASION, Pattern.compile(".*"+occasion+".*" , Pattern.CASE_INSENSITIVE));
		
		DBCursor cursor = occCustEventCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
			Object custEventNm = obj.get(MongoNameConstants.INTERACT_CUSTOME_EVENT);
			if(custEventNm!=null)					
				return custEventNm.toString();
		}
		return null;
	}

}
