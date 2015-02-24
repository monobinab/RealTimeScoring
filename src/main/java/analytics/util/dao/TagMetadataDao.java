package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class TagMetadataDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
	DBCollection tagMetadataCollection;

	/**
	 * t,b,s,po
	 */
	public TagMetadataDao() {
		super();
		tagMetadataCollection = db.getCollection("tagsMetadata");
	}

	public TagMetadata getDetails(String tag){
		BasicDBObject query = new BasicDBObject();
		query.put(Constants.SEG, tag);
		DBObject dbObj = tagMetadataCollection.findOne(query);
		TagMetadata metaDataObj = null;
		if(dbObj!=null && dbObj.containsField(Constants.PURCHASE_OCCASSION)){
			metaDataObj = new TagMetadata();
			metaDataObj.setMdTags(tag);
			metaDataObj.setBusinessUnit((String)dbObj.get(Constants.BUSINESS_UNIT));
			metaDataObj.setSubBusinessUnit((String)dbObj.get(Constants.SUB_BUSINESS_UNIT));
			metaDataObj.setPurchaseOccassion((String)dbObj.get(Constants.PURCHASE_OCCASSION));
		}
		return metaDataObj;
	}
}
