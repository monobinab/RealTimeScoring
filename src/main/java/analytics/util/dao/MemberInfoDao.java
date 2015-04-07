package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MemberInfoDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberInfoDao.class);
	DBCollection memberInfoCollection;

	public MemberInfoDao() {
		super();
		memberInfoCollection = db.getCollection("memberInfo");
	}

	public String getMemberInfoEId(String l_id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBCursor cursor = memberInfoCollection.find(query);
		if (cursor.hasNext()) {
			DBObject obj = cursor.next();
			Object eid = obj.get(MongoNameConstants.E_ID);
			if(eid!=null)					
				return eid.toString();
		}
		return null;
	}

}
