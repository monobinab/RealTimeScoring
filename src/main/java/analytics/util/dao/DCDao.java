package analytics.util.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DCDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(PidDivLnDao.class);
	DBCollection dcQAStrengths, dcModel;

	public DCDao() {
		super();
		dcQAStrengths = db.getCollection(MongoNameConstants.DC_QA_STRENGTHS); // MongoNameConstants.PID_DIV_LN_COLLECTION
		dcModel = db.getCollection("dcModel");// TODO: add constant
	}
	
	public void setDB(DB db){
		this.db = db;
		dcQAStrengths = db.getCollection(MongoNameConstants.DC_QA_STRENGTHS); // MongoNameConstants.PID_DIV_LN_COLLECTION
		dcModel = db.getCollection("dcModel");// TODO: add constant
	}

	public Object getStrength(String promptGroupName, String question_id, String answer_id) {
		BasicDBObject query = new BasicDBObject();
		query.put("q", question_id.toLowerCase());
		query.put("a", answer_id.toLowerCase());
		query.put("c", promptGroupName);
		if (dcQAStrengths != null) {
			DBObject obj = dcQAStrengths.findOne(query);
			if (obj != null) {
				return obj.get("s");
			}
		}
		
		return null;
	}

	public Object getVarName(String promptGroupName) {
		if (dcModel != null) {
			BasicDBObject query = new BasicDBObject();
			query.put("d", promptGroupName);
			DBObject obj = dcModel.findOne(query);
			//System.out.println("varname:"+obj);
			if (obj != null){
				//System.out.println(obj.get("v"));
				return obj.get("v");
			}
		}
		return null;
	}
	//not using this method for now, might need it after change of data-base schema
	public Map<String, List<Object>> getDCModelMap(){
		DBCollection dcModel =  db.getCollection("dcModel");
		Map<String, List<Object>> varModelMap = new HashMap<String, List<Object>>();
    	DBCursor varModelCursor = dcModel.find();
    	for(DBObject varModelObj: varModelCursor) {
    		varModelMap.put((String) varModelObj.get("v"), (List<Object>) varModelObj.get("m"));    		
    	}
    	return varModelMap;
	}

}
