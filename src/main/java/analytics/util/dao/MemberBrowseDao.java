package analytics.util.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import analytics.util.CalendarUtil;
import analytics.util.MongoNameConstants;
import analytics.util.objects.MemberBrowse;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberBrowseDao extends AbstractDao{

    DBCollection memberBrowseCollection;
 
    public MemberBrowseDao(){
    	super();
    	if(db != null) {
    		memberBrowseCollection = db.getCollection("memberBrowse");
    	}
    }
    @SuppressWarnings("unchecked")
  	public MemberBrowse getEntireMemberBrowse(String lId){
      	
      	MemberBrowse entireMemberBrowse = null;
      	DBObject dbo = memberBrowseCollection.findOne(new BasicDBObject("l_id", lId));
      	
      	 if(dbo != null){
      		 entireMemberBrowse = new MemberBrowse();
      		 dbo.removeField("_id");
          	 dbo.removeField("l_id");
          	 Map<String, Map<String, Map<String, Integer>> > dateSpecBuSubBuMap = new HashMap<String, Map<String, Map<String, Integer>>>();
      		 for(String date :  dbo.keySet()){
      			 BasicDBObject dateSpedbObj = (BasicDBObject) dbo.get(date);
      			 Map<String, Map<String, Integer>> browseTagfeedCountsMap = new HashMap<String, Map<String,Integer>>();
	   	    	 for(String browseTag : dateSpedbObj.keySet()){
		    		 Map<String, Integer> feedCountsMap =  (Map<String, Integer>) dateSpedbObj.get(browseTag);
		    		 browseTagfeedCountsMap.put(browseTag, feedCountsMap);
	   	         }
	   	    	dateSpecBuSubBuMap.put(date, browseTagfeedCountsMap);
      		 }
      		entireMemberBrowse.setDateSpecificBuSubBu(dateSpecBuSubBuMap);
      	 }
      	 return entireMemberBrowse;
      }
    
    @SuppressWarnings("unchecked")
  	public MemberBrowse getMemberBrowse7dayHistory(String lId){
      	BasicDBObject filter7daysDBO = new BasicDBObject("l_id",1).append("_id", 0);
    	for(int i=0;i<7;i++) {
    		filter7daysDBO.append(CalendarUtil.getDateFormat().format(new Date().getTime() + (-i * 24 * 60 * 60 * 1000l)),1);
    	}
    	
      	MemberBrowse memberBrowse7day = null;
      	DBObject dbo = memberBrowseCollection.findOne(new BasicDBObject("l_id", lId),filter7daysDBO);
      	
      	 if(dbo != null){
      		 memberBrowse7day = new MemberBrowse();
         	 dbo.removeField("l_id");
          	 Map<String, Map<String, Map<String, Integer>> > dateSpecBuSubBuMap = new HashMap<String, Map<String, Map<String, Integer>>>();
      		 for(String date :  dbo.keySet()){
      			 
      			 BasicDBObject dateSpedbObj = (BasicDBObject) dbo.get(date);
  		    		
		  		 Map<String, Map<String, Integer>> browseTagfeedCountsMap = new HashMap<String, Map<String,Integer>>();
	   	    	 for(String browseTag : dateSpedbObj.keySet()){
		    		 Map<String, Integer> feedCountsMap =  (Map<String, Integer>) dateSpedbObj.get(browseTag);
		    		 browseTagfeedCountsMap.put(browseTag, feedCountsMap);
	   	         }
	   	    	dateSpecBuSubBuMap.put(date, browseTagfeedCountsMap);
      		 }
      		memberBrowse7day.setDateSpecificBuSubBu(dateSpecBuSubBuMap);
      	 }
      	 return memberBrowse7day;
      }

    public void updateMemberBrowse( MemberBrowse memberBrowse, String todayDate){
    	BasicDBObject updateRec = new BasicDBObject();
		BasicDBObject browseTagDbObj = new BasicDBObject();
		Map<String, Map<String, Map<String, Integer>>> dateSpeBuSubBuMap = memberBrowse.getDateSpecificBuSubBu();
		Map<String, Map<String, Integer>> todaySpeBuSubBuMap = dateSpeBuSubBuMap.get(todayDate);
		for(String browseTag : todaySpeBuSubBuMap.keySet()){
			BasicDBObject feedCountdbObj = new BasicDBObject();
			for(String feedType : todaySpeBuSubBuMap.get(browseTag).keySet()){
				feedCountdbObj.append(feedType, todaySpeBuSubBuMap.get(browseTag).get(feedType));
			}
			browseTagDbObj.append(browseTag, feedCountdbObj);
		}
		updateRec.append(todayDate, browseTagDbObj);
		if(!updateRec.isEmpty())
		{
			memberBrowseCollection.update(new BasicDBObject(MongoNameConstants.L_ID,
				memberBrowse.getL_id()), new BasicDBObject("$set", updateRec), true,
				false);
		}
	}
}
