package analytics.util.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberMDTagsDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberMDTagsDao.class);
	DBCollection memberMDTagsCollection;

	public MemberMDTagsDao() {
		super();
		memberMDTagsCollection = db.getCollection("memberMDTags");
	}

	public List<String> getMemberMDTags(String l_id) {
		DBObject dbObj = memberMDTagsCollection.findOne(new BasicDBObject(
				"l_id", l_id));
		List<String> mdTags = (List<String>) dbObj.get("tags");
		return mdTags;
	}

	public void addMemberMDTags(String l_id, List<String> tags) {
		DBObject tagstoUpdate = new BasicDBObject();
		tagstoUpdate.put(MongoNameConstants.L_ID, l_id);
		BasicDBList mdTagsList = new BasicDBList();
		for (String tag : tags) {
			mdTagsList.add(tag);
		}
		tagstoUpdate.put("tags", mdTagsList);
		memberMDTagsCollection.update(new BasicDBObject(
				MongoNameConstants.L_ID, l_id), tagstoUpdate, true, false);
	}
	public void deleteMemberMDTags(String l_id){
		DBObject doc = memberMDTagsCollection.findOne(new BasicDBObject("l_id",l_id)); //get first document
		memberMDTagsCollection.remove(doc);
	}
	/*public static void main(String[] args) {
		MemberMDTagsDao dao = new MemberMDTagsDao();
		List<String> sample = dao.getMemberMDTags("hzuzVKVINbBBen+WGYQT/VJVdwI=");
		System.out.println(sample);
	}*/
}
