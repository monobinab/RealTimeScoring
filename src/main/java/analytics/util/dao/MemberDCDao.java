package analytics.util.dao;

import com.mongodb.util.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberDCDao extends AbstractDao{
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
    DBCollection memberDCCollection;
    public MemberDCDao(){
    	super();
		memberDCCollection = db.getCollection("memberDC");
    }
    
    public void addDateDC(String l_id, String obj_str){
    	DBObject query = new BasicDBObject();
    	DBObject obj = (DBObject)JSON.parse(obj_str);
    	query.put(MongoNameConstants.L_ID,l_id);
    	DBObject object = memberDCCollection.findOne(query);
    	if(object == null){
    		BasicDBList dateDCList = new BasicDBList();
    		dateDCList.add(obj);
    		object = new BasicDBObject();
    		object.put(MongoNameConstants.L_ID, l_id);
    		object.put(MongoNameConstants.MT_DATES_ARR, dateDCList);
    		memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
    	}
    	else{
    		BasicDBList dateDCList = (BasicDBList) object.get(MongoNameConstants.MT_DATES_ARR);
    		dateDCList.add(obj);
    		memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
    	}
    	
    }
    
}
