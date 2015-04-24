package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.MemberInfo;

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

	public MemberInfo getMemberInfo(String l_id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject obj = memberInfoCollection.findOne(query);
		MemberInfo info = new MemberInfo();
		info.setEid(obj.get(MongoNameConstants.E_ID)==null?null:obj.get(MongoNameConstants.E_ID).toString());
		info.setEid(obj.get(MongoNameConstants.E_ID)==null?null:obj.get(MongoNameConstants.E_ID).toString());
		return info;
	}

}
