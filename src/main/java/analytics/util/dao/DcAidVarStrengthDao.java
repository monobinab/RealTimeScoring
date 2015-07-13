package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import analytics.util.MongoNameConstants;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DcAidVarStrengthDao extends AbstractDao{

	DBCollection dcAidVarStrength;
	
	public DcAidVarStrengthDao() {
		super();
		dcAidVarStrength = db.getCollection("dcAidVariableStrength"); // MongoNameConstants.PID_DIV_LN_COLLECTION
	}
	
	public Map<String, Map<String, Integer>> getdcAidVarStrenghtMap(){
		DBCursor modelsCursor = dcAidVarStrength.find();
		Map<String, Map<String, Integer>> dcAidVarStrengthMap = new HashMap<String, Map<String, Integer>>();
		for(DBObject dbObj : modelsCursor){
			Map<String, Integer> varStrengthMap = new HashMap<String, Integer>();
			String aid = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_AID);
			String var = (String) dbObj.get(MongoNameConstants.DC_AID_VAR_MODEL);
			Integer strength = (Integer) dbObj.get(MongoNameConstants.DC_AID_VAR_SCORE);
			varStrengthMap.put(var, strength);
			if(!dcAidVarStrengthMap.containsKey(aid)){
				dcAidVarStrengthMap.put(aid, varStrengthMap);
			}
			else{
				dcAidVarStrengthMap.get(aid).put(var, strength);
			}
		}
		return dcAidVarStrengthMap;
	}
}