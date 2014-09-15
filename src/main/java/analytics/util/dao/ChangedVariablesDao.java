package analytics.util.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;
import analytics.util.objects.Change;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

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
    public Map<String,Change> getMemberVariables(String l_id){
    	SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    	DBObject changedMbrVariables = changedMemberVariablesCollection.findOne(
				new BasicDBObject("l_id", l_id));

		if (changedMbrVariables == null) {
			return null;
		}

		// CREATE MAP FROM VARIABLES TO VALUE (OBJECT)
		Map<String, Change> memberVariablesMap = new HashMap<String, Change>();
		Iterator<String> mbrVariablesIter = changedMbrVariables.keySet().iterator();
		while (mbrVariablesIter.hasNext()) {
			String key = mbrVariablesIter.next();
			if (!key.equals(MongoNameConstants.L_ID) && !key.equals(MongoNameConstants.ID)) {
				DBObject changedMbrVar = (DBObject) changedMbrVariables.get(key);
				if(changedMbrVar!=null && changedMbrVar.get(MongoNameConstants.MV_EFFECTIVE_DATE)!=null &&
						changedMbrVar.get(MongoNameConstants.MV_EXPIRY_DATE)!=null){
				Change cVar;
				try {
					cVar = new Change(
							key,
							changedMbrVar.get(MongoNameConstants.MV_VID),
							simpleDateFormat.parse(changedMbrVar.get(MongoNameConstants.MV_EXPIRY_DATE).toString()),
							simpleDateFormat.parse(changedMbrVar.get(MongoNameConstants.MV_EFFECTIVE_DATE).toString())
							);
				} catch (ParseException e) {
					LOGGER.error("Unable to parse date. Stopping further parse ",e);
					return memberVariablesMap;
				}
				memberVariablesMap.put(key, cVar);
				}
			}
		}
		return memberVariablesMap;
				
	}
}