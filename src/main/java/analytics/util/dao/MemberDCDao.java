package analytics.util.dao;

import java.util.Collection;

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
    	//System.out.println(obj_str);
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
    		for(int i = 0; i < dateDCList.size();i++){
    			DBObject dc_obj = (DBObject) dateDCList.get(i);
    			if(dc_obj.get("d").equals(obj.get("d"))){
    				BasicDBList dclist = (BasicDBList)dc_obj.get("dc");
    				dclist.addAll((BasicDBList)obj.get("dc"));
    			}
    		}
    		memberDCCollection.update(new BasicDBObject(MongoNameConstants.L_ID, l_id), object, true, false);
    	}
    	
    }
    
    public Object getTotalStrength(String category, String l_id, int expiration){
    	System.out.println(l_id);
    	DBObject query = new BasicDBObject();
    	query.put("l_id", l_id);
    	DBObject object = memberDCCollection.findOne(query);
    	Double strengthTotal = 0.0;
    	if(object != null){
    		BasicDBList list = (BasicDBList) object.get("date");
    		for(int i = 0; i < list.size(); i++){
    			DBObject date = (DBObject) list.get(i);
    			BasicDBList dclist = (BasicDBList) date.get("dc");
    			for(int j = 0; j < dclist.size(); j++){
    				DBObject dc = (DBObject) dclist.get(j);
    				strengthTotal += (Integer)(dc.get("s"));
    			}
    		}
    	}
    	System.out.println(strengthTotal);
    	return strengthTotal;
    }
    
}
