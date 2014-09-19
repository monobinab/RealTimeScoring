package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;


public class MemberScoreDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberScoreDao.class);
    DBCollection memberScoreCollection;
    public MemberScoreDao(){
    	super();
		memberScoreCollection = db.getCollection("memberScore");
    }
    
    public Map<String,String> getMemberScores(String l_id){
    	Map<String,String> memberScores = new HashMap<String, String>();
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.L_ID, l_id);
		DBObject dbObj = memberScoreCollection.findOne(query);

		if (dbObj != null && dbObj.keySet() != null) {
			for (String key : dbObj.keySet()) {
				// skip expired changes
				if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key)) {
					continue;
				}
				else{
					memberScores.put(key, dbObj.get(key).toString());
				}
			}
		}
		return memberScores;
				
	}
}
