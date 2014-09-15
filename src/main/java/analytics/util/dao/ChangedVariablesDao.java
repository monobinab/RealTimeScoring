package analytics.util.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;

public class ChangedVariablesDao {
	static final Logger LOGGER = LoggerFactory
			.getLogger(ChangedVariablesDao.class);
	static DB db;
    DBCollection changedMemberVariablesCollection;
    static {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			LOGGER.error("Unable to get DB connection", e);
		}
    }
    public ChangedVariablesDao(){
		changedMemberVariablesCollection = db.getCollection("changedMemberVariables");
    }
	public void upsertUpdateChangedScores(String lId, BasicDBObject newDocument) {
		BasicDBObject searchQuery = new BasicDBObject().append(MongoNameConstants.L_ID,
				lId);
		changedMemberVariablesCollection.update(searchQuery,
				new BasicDBObject("$set", newDocument), true, false);
		
	}
}
