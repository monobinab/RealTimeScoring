package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import analytics.util.MongoNameConstants;
import analytics.util.objects.MemberBrowse;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberBrowseDao extends AbstractDao{

    DBCollection memberBrowseCollection;
    
    public MemberBrowseDao(){
    	super();
    	memberBrowseCollection = db.getCollection("memberBrowse");
    }
   
    @SuppressWarnings("unchecked")
	public MemberBrowse getMemberBrowse(String lId, String date){
    	MemberBrowse memberBrowse = new MemberBrowse();
    	memberBrowse.setL_id(lId);
	    memberBrowse.setDate(date);
    	 DBObject dbo = memberBrowseCollection.findOne(new BasicDBObject("l_id", lId));
    	 if(dbo != null){
	    	 BasicDBObject dbObjToday = (BasicDBObject) dbo.get(date);
	    	 if(dbObjToday != null)
	    	 {	
	    		 Map<String, Map<String, Integer>> browseTagfeedCountsMap = new HashMap<String, Map<String,Integer>>();
	   	    	 for(String browseTag : dbObjToday.keySet()){
	    		 Map<String, Integer> feedCountsMap =  (Map<String, Integer>) dbObjToday.get(browseTag);
	    		 browseTagfeedCountsMap.put(browseTag, feedCountsMap);
	   	    	}
	   	      	memberBrowse.setTags(browseTagfeedCountsMap);
	    	 }
    	 }
    	 return memberBrowse;
    }
    
	public void updateMemberBrowse( MemberBrowse memberBrowse){
		BasicDBObject updateRec = new BasicDBObject();
		BasicDBObject browseTagDbObj = new BasicDBObject();
		
		for(String browseTag : memberBrowse.getTags().keySet()){
			BasicDBObject feedCountdbObj = new BasicDBObject();
			for(String feedType : memberBrowse.getTags().get(browseTag).keySet()){
				feedCountdbObj.append(feedType, memberBrowse.getTags().get(browseTag).get(feedType));
			}
			browseTagDbObj.append(browseTag, feedCountdbObj);
		}
		updateRec.append(memberBrowse.getDate(), browseTagDbObj);
		if(!updateRec.isEmpty())
		{
			memberBrowseCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				memberBrowse.getL_id()), new BasicDBObject("$set", updateRec), true,
				false);
		}
	}
}
