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

public class DivLineBuSubDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
	DBCollection divLineBuSubCollection;

	/**
	 * t,b,s,po
	 */
	public DivLineBuSubDao() {
		super();
		divLineBuSubCollection = db.getCollection("divLnBuName");
		LOGGER.info("collection in tagMetadataDao: " + divLineBuSubCollection.getFullName());
	}


	public TagMetadata getBuSubBu(TagMetadata tagMetadata,String divLine){
		
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.DLBS_DIV, divLine);
		
		DBObject dbObj = divLineBuSubCollection.findOne(query);
		
		if(dbObj != null){

			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_BU) : (String)dbObj.get(MongoNameConstants.DLBS_BU));
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_SUB) : (String)dbObj.get(MongoNameConstants.DLBS_SUB));
	
		}
		return tagMetadata;
	}
}
