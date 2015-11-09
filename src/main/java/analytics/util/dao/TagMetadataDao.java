package analytics.util.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.util.matching.Regex;
import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
		LOGGER.info("colelction in tagMetadataDao: " + tagMetadataCollection.getFullName());
	}

	public TagMetadata getDetails(String tag){
		BasicDBObject query = new BasicDBObject();
		query.put(Constants.SEG, tag);
		DBObject dbObj = tagMetadataCollection.findOne(query);
		TagMetadata metaDataObj = null;
		if(dbObj!=null && dbObj.containsField(MongoNameConstants.PURCHASE_OCCASSION)){
			metaDataObj = new TagMetadata();
			metaDataObj.setMdTag(tag);
			metaDataObj.setBusinessUnit((String)dbObj.get(MongoNameConstants.BUSINESS_UNIT));
			metaDataObj.setSubBusinessUnit((String)dbObj.get(MongoNameConstants.SUB_BUSINESS_UNIT));
			metaDataObj.setPurchaseOccassion((String)dbObj.get(MongoNameConstants.PURCHASE_OCCASSION));
		}
		return metaDataObj;
	}

	public ArrayList<TagMetadata> getDetailsList(String tags){
		BasicDBList list = new BasicDBList();
		String[] tagsArr = tags.split(",");
    	for(String tag:tagsArr){
    		list.add(tag);
    	}
		BasicDBObject query = new BasicDBObject();
		query.put(Constants.SEG, new BasicDBObject("$in", list));
		DBCursor dbCursor = tagMetadataCollection.find(query);
		DBObject record = null;
		TagMetadata metaDataObj = null;
		ArrayList<TagMetadata> tagMetaDataList = new ArrayList<TagMetadata>();
		
		while (dbCursor.hasNext()) {
			record = dbCursor.next();
			
			if(record!=null && record.containsField(MongoNameConstants.PURCHASE_OCCASSION)){
				metaDataObj = new TagMetadata();
				metaDataObj.setMdTag((String)record.get(MongoNameConstants.SEG));
				metaDataObj.setBusinessUnit((String)record.get(MongoNameConstants.BUSINESS_UNIT));
				metaDataObj.setSubBusinessUnit((String)record.get(MongoNameConstants.SUB_BUSINESS_UNIT));
				metaDataObj.setPurchaseOccassion((String)record.get(MongoNameConstants.PURCHASE_OCCASSION));
				metaDataObj.setFirst5CharMdTag(((String)record.get(MongoNameConstants.SEG)).substring(0, 5));
				tagMetaDataList.add(metaDataObj);
			}
		}
		return tagMetaDataList;
	}
	public TagMetadata getBuSubBu(String tag){
		
      	//char[] charArr = tag.toCharArray();
		//Pattern namesRegex = Pattern.compile("^"+charArr[0]+charArr[1]+charArr[2]+charArr[3]+charArr[4]+".*");
      	Pattern namesRegex = Pattern.compile("^"+tag.substring(0, 5)+".*");
		BasicDBObject query = new BasicDBObject(Constants.SEG, namesRegex);
		//query.put(Constants.SEG, namesRegex);
		DBObject dbObj = tagMetadataCollection.findOne(query);
		TagMetadata metaDataObj = null;
		if(dbObj != null){
			metaDataObj = new TagMetadata();
			//zeros added to indicate Unknown
			//needs to be replaced when proper collection is created 
			//metaDataObj.setMdTags(tag+"0000000000");
			metaDataObj.setMdTag((String)dbObj.get(MongoNameConstants.SEG));
			metaDataObj.setBusinessUnit((String)dbObj.get(MongoNameConstants.BUSINESS_UNIT));
			metaDataObj.setSubBusinessUnit((String)dbObj.get(MongoNameConstants.SUB_BUSINESS_UNIT));
			metaDataObj.setPurchaseOccassion((String)dbObj.get(MongoNameConstants.PURCHASE_OCCASSION));
		}
		return metaDataObj;
	}
	
	public void addTagMetaData(TagMetadata metaDataObj){
		BasicDBObject newObj = new BasicDBObject();
		newObj.append("SUB", metaDataObj.getSubBusinessUnit());
		newObj.append("BU_", metaDataObj.getBusinessUnit());
		newObj.append("SEG", metaDataObj.getMdTag());
		newObj.append("OCC", metaDataObj.getPurchaseOccasion());
		tagMetadataCollection.insert(newObj);
		
	}
}
