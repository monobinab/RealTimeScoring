package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class TagMetadataDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
	DBCollection tagMetadataCollection;

	public TagMetadataDao() {
		super();
		tagMetadataCollection = db.getCollection("tagMetadata");
	}

	public TagMetadata getDetails(String tag){
		BasicDBObject query = new BasicDBObject();
		query.put("t", tag);
		DBObject dbObj = tagMetadataCollection.findOne(query);
		
		return null;
		
	}
}
