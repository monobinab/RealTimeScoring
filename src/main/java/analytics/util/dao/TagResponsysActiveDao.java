package analytics.util.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class TagResponsysActiveDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
	DBCollection tagResponsysActiveCollection;

	/**
	 * t,b,s,po
	 */
	public TagResponsysActiveDao() {
		super();
		tagResponsysActiveCollection = db.getCollection("tagsResponsysActive");
		LOGGER.info("colelction in tagMetadataDao: " + tagResponsysActiveCollection.getFullName());
	}

	public HashSet<String> getResponsysActiveTagsList(){

		DBCursor dbCursor = tagResponsysActiveCollection.find();
		DBObject record = null;
		HashSet<String> activeTagsList = new HashSet<String>();
		
		while (dbCursor.hasNext()) {
			record = dbCursor.next();
			if(record!=null){
				activeTagsList.add((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT));
			}
		}
		return activeTagsList;
	}
	
	public List<String> getActiveResponsysTagsList(){
		DBCursor dbCursor = tagResponsysActiveCollection.find();
		DBObject dbObj = null;
		List<String> activeTags = new ArrayList<String>();
		while(dbCursor.hasNext()){
			dbObj = dbCursor.next();
			if(dbObj != null){
			if(dbObj.get("OCC").toString().equalsIgnoreCase("Unknown")){
				activeTags.add((String) dbObj.get("BU"));
				}
			}
		}
			return activeTags;
	}
}

