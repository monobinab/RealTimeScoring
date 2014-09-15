package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ChangedMemberScoresDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(ChangedMemberScoresDao.class);
	static DB db;
    DBCollection changedMemberScoresCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public ChangedMemberScoresDao(){
    	changedMemberScoresCollection = db.getCollection("changedMemberScores");
    }
	public void upsertUpdateChangedScores(String lId, BasicDBObject updateRec) {
		changedMemberScoresCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				lId), new BasicDBObject("$set", updateRec), true,
				false);
		
	}
}
