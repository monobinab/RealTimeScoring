package analytics.util.dao;

import analytics.util.DBConnection;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class FbVariableDao {
//DBObject fbVar = fbVariableCollection.findOne(new BasicDBObject("k",topic));
	//fbVar.get("v"
	
	DB db;
    DBCollection fbVariableCollection;
    {
		try {
			db = DBConnection.getDBConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fbVariableCollection = db.getCollection("fbVariable");
    }

	public String getVariableFromTopic(String topic){
		BasicDBObject query = new BasicDBObject();
		query.put(MongoNameConstants.FB_KEYWORD, topic);
		DBObject obj = fbVariableCollection.findOne(query);
		if (obj!=null) {
		    return obj.get(MongoNameConstants.FB_VARIABLE).toString();
		}
		return null;
	}
}
