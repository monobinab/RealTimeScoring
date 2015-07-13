package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.MemberInfo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
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
		if(obj==null)
			return null;
		MemberInfo info = new MemberInfo();
		info.setEid(obj.get(MongoNameConstants.E_ID)==null?null:obj.get(MongoNameConstants.E_ID).toString());
		info.setEmailOptIn(obj.get(MongoNameConstants.EMAIL_OPT_IN)==null?null:obj.get(MongoNameConstants.EMAIL_OPT_IN).toString());
		info.setState(obj.get(MongoNameConstants.STATE)==null?null:obj.get(MongoNameConstants.STATE).toString());
		info.setSrs_opt_in(obj.get(MongoNameConstants.SEARS_OPT_IN)==null?null:obj.get(MongoNameConstants.SEARS_OPT_IN).toString());
		info.setKmt_opt_in(obj.get(MongoNameConstants.KMART_OPT_IN)==null?null:obj.get(MongoNameConstants.KMART_OPT_IN).toString());
		info.setSyw_opt_in(obj.get(MongoNameConstants.SYW_OPT_IN)==null?null:obj.get(MongoNameConstants.SYW_OPT_IN).toString());
		return info;
	}

}
