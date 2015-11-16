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
	private final String filterHolidayBUSUbBU = "Electronics|Sears Camera|Electronics|Sears Television|Lawn & Garden|"
			+ "Sears Mower|Lawn & Garden|Sears small items|Lawn & Garden|Sears Trimmers|outdoor Living|Sears Grill|"
			+ "outdoor Living|Sears Patio Furniture|Sporting Goods|Sears Gameroom|Tools|Sears Hand tools|Tools|"
			+ "Sears Power Tools|Tools|Sears Tool Storage";

	/**
	 * t,b,s,po
	 */
	public DivLineBuSubDao() {
		super();
		divLineBuSubCollection = db.getCollection("divLnBuName");
		LOGGER.info("collection in tagMetadataDao: " + divLineBuSubCollection.getFullName());
	}


/*	public TagMetadata getBuSubBu(TagMetadata tagMetadata,String divLine){
		
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.DLBS_DIV, divLine);
		
		DBObject dbObj = divLineBuSubCollection.findOne(query);
		
		if(dbObj != null){
			tagMetadata.setDivLine(tagMetadata.getDivLine()!=null ? tagMetadata.getDivLine()+"," +divLine: divLine);
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_BU) : (String)dbObj.get(MongoNameConstants.DLBS_BU));
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_SUB) : (String)dbObj.get(MongoNameConstants.DLBS_SUB));
	
		}else{
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+",null" : "null");
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+",null" : "null");
		}
		return tagMetadata;
	}*/
	
	//Temporary fix to supress certain BU, SubBUs for sending emails... Holiday Season only...
	public TagMetadata getBuSubBuHolidaySeason(TagMetadata tagMetadata,String divLine){
		
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.DLBS_DIV, divLine);
		
		DBObject dbObj = divLineBuSubCollection.findOne(query);
		
		if(dbObj != null){
			String buSubBu = (String)dbObj.get(MongoNameConstants.DLBS_BU)+"|"+(String)dbObj.get(MongoNameConstants.DLBS_SUB);
			if(filterHolidayBUSUbBU.toLowerCase().contains(buSubBu.toLowerCase()))
				return tagMetadata;
				
			tagMetadata.setDivLine(tagMetadata.getDivLine()!=null ? tagMetadata.getDivLine()+"," +divLine: divLine);
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_BU) : (String)dbObj.get(MongoNameConstants.DLBS_BU));
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+","+(String)dbObj.get(MongoNameConstants.DLBS_SUB) : (String)dbObj.get(MongoNameConstants.DLBS_SUB));
	
		}/*else{
			tagMetadata.setBusinessUnit(tagMetadata.getBusinessUnit()!=null ? tagMetadata.getBusinessUnit()+",null" : "null");
			tagMetadata.setSubBusinessUnit(tagMetadata.getSubBusinessUnit() != null ? tagMetadata.getSubBusinessUnit()+",null" : "null");
		}*/
		return tagMetadata;
	}
}
