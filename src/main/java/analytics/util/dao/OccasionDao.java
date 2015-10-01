package analytics.util.dao;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.exception.RealTimeScoringException;
import analytics.util.objects.OccasionInfo;

public class OccasionDao extends AbstractMongoDao{
	
	private DBCollection occasionInfoCollection;
	private List<OccasionInfo> occasionInfos = new ArrayList<OccasionInfo>();
	private static OccasionDao occasionDao;
	
	private OccasionDao() {
		super();
		try {
			occasionInfos = this.getOccasionsInfo();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static OccasionDao getInstance(){
        if(occasionDao == null){
        	occasionDao = new OccasionDao();
        }
        return occasionDao;
    }
	
	public List<OccasionInfo> getOccasionsInfo() throws Exception{
	//	List<OccasionInfo> occasionInfos = new ArrayList<OccasionInfo>();
		  if(occasionInfos!= null && occasionInfos.size()>0){
			  return occasionInfos;
		  }
		occasionInfoCollection = db.getCollection("cpsOccasions");
		if(occasionInfoCollection != null){
			DBCursor cursor = occasionInfoCollection.find();
			cursor.sort(new BasicDBObject("priority", 1));
			if (cursor != null && cursor.size() > 0) {
				while (cursor.hasNext()) {
					DBObject obj = cursor.next();
					if(obj != null){
						OccasionInfo occasionInfo = new OccasionInfo();
						occasionInfo.setOccasion((String)obj.get("occasion"));
						occasionInfo.setPriority((String)obj.get("priority"));
						occasionInfo.setDuration((String)obj.get("duration"));
						occasionInfo.setDaysToCheckInHistory((String)obj.get("daysInHistory"));
						occasionInfo.setIntCustEvent("");
						occasionInfos.add(occasionInfo);
					}
				}
			}
		}
		if(occasionInfos != null && occasionInfos.size() > 0){
			occasionInfoCollection = db.getCollection("occ_cust_event");
			if(occasionInfoCollection != null){
				
				for(OccasionInfo occasionInfo : occasionInfos){
					DBCursor cursor = occasionInfoCollection.find();
					if(cursor != null && cursor.size() > 0 && occasionInfo != null){
						while(cursor.hasNext()) {
							DBObject dbObj = cursor.next();
							if(dbObj != null){
								String occ = (String)dbObj.get("occasion");
								if(StringUtils.isNotEmpty(occ) && occ.equalsIgnoreCase(occasionInfo.getOccasion())){
									occasionInfo.setIntCustEvent((String)dbObj.get("intCustEvent"));
									occasionInfo.setCustEventId((Integer)dbObj.get("custEventId"));
									break;
								}
							}
						}
					}
				}
			}
		}
		System.out.println("OccasionInfos size = " + occasionInfos.size()); 
		return occasionInfos;
	}
	
	public OccasionInfo getOccasionInfo(String occ){
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getOccasion().equalsIgnoreCase(occ)){
					return occasionInfo;
				}
			}
		}
		return null;
	}
	
	public OccasionInfo getOccasionInfoFromPriority(String priority){
		if(occasionInfos != null && occasionInfos.size() > 0){
			for(OccasionInfo occasionInfo : occasionInfos){
				if(occasionInfo != null && occasionInfo.getPriority().equalsIgnoreCase(priority)){
					return occasionInfo;
				}
			}
		}
		return null;
	} 
}
