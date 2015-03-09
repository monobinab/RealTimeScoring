package analytics.bolt;

import org.apache.commons.configuration.ConfigurationException;

import analytics.util.DBConnection;
import analytics.util.FakeMongo;

import com.github.fakemongo.Fongo;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ParsingBoltOccassionFakeMonogColl {
	static DB db;
	static DBCollection memberMDTagsColl;
	static DBCollection tagMetadataColl;
	static DBCollection tagVariableColl;
	static DBCollection modelPercColl;
	
	public static DBCollection getModelPercColl() {
		return modelPercColl;
	}

	public static void setModelPercColl(DBCollection modelPercColl) {
		ParsingBoltOccassionFakeMonogColl.modelPercColl = modelPercColl;
	}

	public static DBCollection getMemberMDTagsColl() {
		return memberMDTagsColl;
	}

	public static void setMemberMDTagsColl(DBCollection memberMDTagsColl) {
		ParsingBoltOccassionFakeMonogColl.memberMDTagsColl = memberMDTagsColl;
	}

	public static DBCollection getTagMetadataColl() {
		return tagMetadataColl;
	}

	public static void setTagMetadataColl(DBCollection tagMetadataColl) {
		ParsingBoltOccassionFakeMonogColl.tagMetadataColl = tagMetadataColl;
	}


	public static DBCollection getTagVariableColl() {
		return tagVariableColl;
	}

	public static void setTagVariableColl(DBCollection tagVariableColl) {
		ParsingBoltOccassionFakeMonogColl.tagVariableColl = tagVariableColl;
	}

	public static void fakeMongoColl() throws ConfigurationException{
	System.setProperty("rtseprod", "test");
	  FakeMongo.setDBConn(new Fongo("test db").getDB("test"));	
		db = DBConnection.getDBConnection();
		
		//fake memberMDTags collection
		memberMDTagsColl = db.getCollection("memberMdTags");
		BasicDBList list = new BasicDBList();
		list.add("HACKS2010");
		list.add("HARFS2010");
		list.add("HALAS2010");
		list.add("HADHS2010");
		memberMDTagsColl.insert(new BasicDBObject("l_id", "OccassionTopologyTestingl_id").append("tags", list));
		
		//fake tagMetaData collection
		tagMetadataColl = db.getCollection("tagsMetadata");
		tagMetadataColl.insert(new BasicDBObject("SEG", "HACKS2010").append("BRA", "Nikon").append("BU_", "HA").append("SUB", "CK").append("OCC", "Duress"));
		tagMetadataColl.insert(new BasicDBObject("SEG", "HARFS2010").append("BRA", "LG").append("BU_", "HA").append("SUB", "RF").append("OCC", "Moving"));
		tagMetadataColl.insert(new BasicDBObject("SEG", "HALAS2010").append("BRA", "Unknown").append("BU_", "HA").append("SUB", "LA").append("OCC", "Duress"));
		
		//fake tagVariable collection
		tagVariableColl = db.getCollection("tagVariable");
		tagVariableColl.insert(new BasicDBObject("t", "HACKS").append("v", "BOOST_PO_HA_COOK_TEST").append("m", 35));
		tagVariableColl.insert(new BasicDBObject("t", "HARFS").append("v", "BOOST_PO_HA_REF_TEST").append("m", 39));
		tagVariableColl.insert(new BasicDBObject("t", "HALAS").append("v", "BOOST_PO_HA_LA_TEST").append("m", 46));
		
		//fake modelPerc collection
		modelPercColl = db.getCollection("modelPercentile");
		modelPercColl.insert(new BasicDBObject("modelId", "68").append("modelName", "S_SCR_TEST").append("percentile", "99").append("maxScore", 0.11));
		modelPercColl.insert(new BasicDBObject("modelId", "35").append("modelName", "S_SCR_TEST2").append("percentile", "99").append("maxScore", 0.11));
		modelPercColl.insert(new BasicDBObject("modelId", "39").append("modelName", "S_SCR_TEST3").append("percentile", "99").append("maxScore", 0.11));
		modelPercColl.insert(new BasicDBObject("modelId", "46").append("modelName", "S_SCR_TEST4").append("percentile", "99").append("maxScore", 0.11));
		
		setTagMetadataColl(tagMetadataColl);
		setMemberMDTagsColl(memberMDTagsColl);
		setModelPercColl(modelPercColl);
		setTagVariableColl(tagVariableColl);
	}	

}
