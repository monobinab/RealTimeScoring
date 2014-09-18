package analytics.util.dao;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.ChangedMemberScore;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

public class ChangedMemberScoresDao extends AbstractDao{
	static final Logger LOGGER = LoggerFactory
			.getLogger(ChangedMemberScoresDao.class);

    DBCollection changedMemberScoresCollection;
    
    public ChangedMemberScoresDao(){
    	super();
    	changedMemberScoresCollection = db.getCollection("changedMemberScores");
    }
	public void upsertUpdateChangedScores(String lId, Map<Integer, ChangedMemberScore> updatedScores) {
		BasicDBObject updateRec = new BasicDBObject();
		for(Integer modelId: updatedScores.keySet()){
			ChangedMemberScore scoreObj = updatedScores.get(modelId);
			if(scoreObj!=null){
			updateRec.append(
					modelId.toString(),
					new BasicDBObject()
							.append(MongoNameConstants.CMS_SCORE, scoreObj.getScore())
							.append(MongoNameConstants.CMS_MIN_EXPIRY_DATE,scoreObj.getMinDate())
							.append(MongoNameConstants.CMS_MAX_EXPIRY_DATE,scoreObj.getMaxDate())
							.append(MongoNameConstants.CMS_EFFECTIVE_DATE, scoreObj.getEffDate()));
		}}

		changedMemberScoresCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				lId), new BasicDBObject("$set", updateRec), true,
				false);
		
	}
}
