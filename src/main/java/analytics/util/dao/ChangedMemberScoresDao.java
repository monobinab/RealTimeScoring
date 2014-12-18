package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.ChangedMemberScore;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class ChangedMemberScoresDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(ChangedMemberScoresDao.class);

    DBCollection changedMemberScoresCollection;
    
    public ChangedMemberScoresDao(){
    	super();
    	changedMemberScoresCollection = db.getCollection("changedMemberScores");
    }
    
    public void deleteChangedMemberScore(String lId){
    	changedMemberScoresCollection.findAndRemove(new BasicDBObject("l_id",lId));
    }
    
	public void upsertUpdateChangedScores(String lId, Map<Integer, ChangedMemberScore> updatedScores) {
		SimpleDateFormat timestampForMongo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		String timeStamp = timestampForMongo.format(new Date());
		BasicDBObject updateRec = new BasicDBObject("t",timeStamp);
		for(Integer modelId: updatedScores.keySet()){
			ChangedMemberScore scoreObj = updatedScores.get(modelId);
			if(scoreObj!=null){
			updateRec.append(
					modelId.toString(),
					new BasicDBObject()
							.append(MongoNameConstants.CMS_SCORE, scoreObj.getScore())
							.append(MongoNameConstants.CMS_MIN_EXPIRY_DATE,scoreObj.getMinDate())
							.append(MongoNameConstants.CMS_MAX_EXPIRY_DATE,scoreObj.getMaxDate())
							.append(MongoNameConstants.CMS_EFFECTIVE_DATE, scoreObj.getEffDate())
							.append(MongoNameConstants.CMS_SOURCE, scoreObj.getSource()));
		}}
		if(!updateRec.isEmpty())
		{
			changedMemberScoresCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				lId), new BasicDBObject("$set", updateRec), true,
				false);
		}
		
	}
	
	public Map<String,ChangedMemberScore> getChangedMemberScores(String l_id){
    	Map<String,ChangedMemberScore> memberScores = new HashMap<String, ChangedMemberScore>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject dbObj = changedMemberScoresCollection.findOne(query);

		if (dbObj != null && dbObj.keySet() != null) {
			for (String key : dbObj.keySet()) {
				// skip expired changes
				if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key)) {
					continue;
				}
				else{
					DBObject scoreObj = (DBObject) dbObj.get(key);
					if(scoreObj!=null && scoreObj.get(MongoNameConstants.CMS_MIN_EXPIRY_DATE)!=null && 
							scoreObj.get(MongoNameConstants.CMS_MAX_EXPIRY_DATE)!=null){
						ChangedMemberScore score = new ChangedMemberScore((Double)scoreObj.get("s"), (String)scoreObj.get("minEx"), (String)scoreObj.get("maxEx"), (String)scoreObj.get("f"), (String)scoreObj.get("c"));
						memberScores.put(key, score);
					}
				}
			}
		}
		return memberScores;
				
	}
}
