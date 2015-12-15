package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.ChangedMemberScore;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MemberScoreDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberScoreDao.class);
    DBCollection memberScoreCollection;
    public MemberScoreDao(){
    	//Connect to secondary server
    	//super("server2");
    	super("static");
		memberScoreCollection = db.getCollection("memberScore");
    }
    
    public Map<String,String> getMemberScores(String l_id){
    	Map<String,String> memberScores = new HashMap<String, String>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject dbObj = memberScoreCollection.findOne(query);
		
		if (dbObj != null && dbObj.keySet() != null) {
			for (String key : dbObj.keySet()) {
				try{
				// skip expired changes
				if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key) || MongoNameConstants.TIMESTAMP.equals(key)) {
					continue;
				}
				else{
					memberScores.put(key, dbObj.get(key).toString());
				}
			}
				catch(Exception e){
					LOGGER.error("Exception in memberScoreDao for " + l_id);
				//	memberScores.put(key, "0");
					
				}
			}
			
		}
		
		
		return memberScores;
				
	}
    
    public Map<String,Double> getMemberScores2(String l_id){
    	Map<String,Double> memberScores = new HashMap<String, Double>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject dbObj = memberScoreCollection.findOne(query);

		if (dbObj != null && dbObj.keySet() != null) {
			for (String key : dbObj.keySet()) {
				// skip expired changes
				if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key) || MongoNameConstants.TIMESTAMP.equals(key)) {
					continue;
				}
				else{
					memberScores.put(key, Double.parseDouble(dbObj.get(key).toString()) );
				}
			}
		}else{
			query.put(MongoNameConstants.L_ID, "defaultMember");
			dbObj = memberScoreCollection.findOne(query);

			if (dbObj != null && dbObj.keySet() != null) {
				for (String key : dbObj.keySet()) {
					// skip expired changes
					if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key) || MongoNameConstants.TIMESTAMP.equals(key)) {
						continue;
					}
					else{
						memberScores.put(key, Double.parseDouble(dbObj.get(key).toString()) );
					}
				}
			}			
			
		}
		return memberScores;
				
	}
    
    public void upsertUpdateMemberScores(String lId, Map< String, Double> memberScoresList) {

		JSONObject json = new JSONObject(memberScoresList);
		Object o = com.mongodb.util.JSON.parse(json.toString());
		DBObject dbObj = (DBObject) o;
	
		memberScoreCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				lId), new BasicDBObject("$set", dbObj), true,
				false);
		
	}

	public Long getMemberInfoCount(String l_id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		return memberScoreCollection.count(query);
	}

	public void deleteMemberScore(String l_id) {
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		memberScoreCollection.remove(query);
		
	}
}
