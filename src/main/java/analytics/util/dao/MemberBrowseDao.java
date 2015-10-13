package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import analytics.util.MongoNameConstants;
import analytics.util.objects.DateSpecificMemberBrowse;
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
	public DateSpecificMemberBrowse getMemberBrowse(String lId, String date){
    	DateSpecificMemberBrowse memberBrowse = new DateSpecificMemberBrowse();
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
	   	      	memberBrowse.setBuSubBu(browseTagfeedCountsMap);
	    	 }
    	 }
    	 return memberBrowse;
    }
    
    
    @SuppressWarnings("unchecked")
	public MemberBrowse getEntireMemberBrowse(String lId){
    	
    	MemberBrowse entireMemberBrowse = new MemberBrowse();
    	Map<String, DateSpecificMemberBrowse> dateSpecificMemberBrowse = new HashMap<String, DateSpecificMemberBrowse>();
    	 DBObject dbo = memberBrowseCollection.findOne(new BasicDBObject("l_id", lId));
    	 dbo.removeField("_id");
    	 dbo.removeField("l_id");
    	 if(dbo != null){
    		 for(String date :  dbo.keySet()){
    			 
    			 BasicDBObject dateSpedbObj = (BasicDBObject) dbo.get(date);
		    	 if(dateSpedbObj != null)
			    	 {	
		    		 	 DateSpecificMemberBrowse memberBrowse = new DateSpecificMemberBrowse();
			    		 Map<String, Map<String, Integer>> browseTagfeedCountsMap = new HashMap<String, Map<String,Integer>>();
				   	    	 for(String browseTag : dateSpedbObj.keySet()){
					    		 Map<String, Integer> feedCountsMap =  (Map<String, Integer>) dateSpedbObj.get(browseTag);
					    		 browseTagfeedCountsMap.put(browseTag, feedCountsMap);
				   	         }
			   	      	memberBrowse.setBuSubBu(browseTagfeedCountsMap);
			   	      	dateSpecificMemberBrowse.put(date, memberBrowse);
			    	 }
		    	 		entireMemberBrowse.setMemberBrowse(dateSpecificMemberBrowse);
			  	 }
    	 }
    	 return entireMemberBrowse;
    }
    
	public void updateMemberBrowse(String l_id,  DateSpecificMemberBrowse memberBrowse){
		BasicDBObject updateRec = new BasicDBObject();
		BasicDBObject browseTagDbObj = new BasicDBObject();
		
		for(String browseTag : memberBrowse.getBuSubBu().keySet()){
			BasicDBObject feedCountdbObj = new BasicDBObject();
			for(String feedType : memberBrowse.getBuSubBu().get(browseTag).keySet()){
				feedCountdbObj.append(feedType, memberBrowse.getBuSubBu().get(browseTag).get(feedType));
			}
			browseTagDbObj.append(browseTag, feedCountdbObj);
		}
		updateRec.append(memberBrowse.getDate(), browseTagDbObj);
		if(!updateRec.isEmpty())
		{
			memberBrowseCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				l_id), new BasicDBObject("$set", updateRec), true,
				false);
		}
	}
}
