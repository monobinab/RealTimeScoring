package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.BrowseTag;
import analytics.util.objects.MemberBrowse;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MemberBrowseDao extends AbstractDao{

	private static final Logger LOGGER = LoggerFactory
			.getLogger(BoostDao.class);
    DBCollection memberBrowseCollection;
    
    public MemberBrowseDao(){
    	super();
    	memberBrowseCollection = db.getCollection("memberBrowse");
    }
    
    public MemberBrowse getMemberBrowse(String l_id){
    	DBObject memberBrowseObj = memberBrowseCollection.findOne(new BasicDBObject("l_id", l_id));
    	MemberBrowse memberBrowse = null;
    	if(memberBrowseObj != null ){
    		memberBrowse = new MemberBrowse();
    		Map<String, List<BrowseTag>> dateSpecificBrowseTags = new HashMap<String, List<BrowseTag>>();
    		memberBrowse.setlId((String) memberBrowseObj.get(MongoNameConstants.L_ID));
    		for (String key : memberBrowseObj.keySet()) {
    		
    			if (MongoNameConstants.L_ID.equals(key) || MongoNameConstants.ID.equals(key) ) {
					continue;
				}
    		
    			else{
	    			DBObject tagsList = (DBObject) memberBrowseObj.get(key);
	    			List<BrowseTag> browseTagsList = new ArrayList<BrowseTag>();
	    			for(String tag : tagsList.keySet()){
	    				BrowseTag browseTag = new BrowseTag();
	    				browseTag.setBrowseTag(tag);
	    				Set<String> feeds = ((DBObject) tagsList.get(tag)).keySet();
	    				Map<String, Object> feedCountsMap = new HashMap<String, Object>();
	    				for(String feed : feeds){
	    					int count =  (Integer) ((DBObject) tagsList.get(tag)).get(feed);
	    					feedCountsMap.put(feed, (Integer) ((DBObject) tagsList.get(tag)).get(feed));
	    				}
	    				browseTag.setFeedCounts(feedCountsMap);
	    				browseTagsList.add(browseTag);
	    			}
	    			dateSpecificBrowseTags.put(key, browseTagsList);
     			}
    		}	
    		memberBrowse.setBrowseTags(dateSpecificBrowseTags);
    	}
		return memberBrowse;
    }
}
