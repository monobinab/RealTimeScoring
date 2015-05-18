package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class OccasionResponsesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OccasionResponsesDao.class);
	DBCollection occasionResonsesCollection;

	public OccasionResponsesDao() {
		super();
		occasionResonsesCollection = db.getCollection("occasionResponses");
	}


	public void addOccasionResponse(String l_id, String eid, String custEvent, String purOcca, String businessUnit, 
			String subBusUnit, String successFlag, String tag, String topologyName) {
		
		DBObject occObj = new BasicDBObject();
		occObj.put(MongoNameConstants.L_ID, l_id);
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
		occObj.put(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		occObj.put("eid", eid);
		occObj.put("custEvent", custEvent);
		if(!topologyName.equalsIgnoreCase("Telluride"))
			occObj.put("purchaseOccasion", purOcca);
		//occObj.put("businessUnit", businessUnit);
		//occObj.put("subBusinessUnit", subBusUnit);
		occObj.put("successFlag", successFlag);
		occObj.put("tag", tag);
		
		occasionResonsesCollection.insert(occObj);
	
	}
	
	/*public void addOccasionResponseUnknown(String l_id, String eid, String custEvent, String purOcca, String businessUnit, 
			String subBusUnit, String successFlag, String tag, String topologyName) {
		
		DBObject occObj = new BasicDBObject();
		occObj.put(MongoNameConstants.L_ID, l_id);
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
		occObj.put(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		occObj.put("eid", eid);
		occObj.put("custEvent", custEvent);
		//occObj.put("businessUnit", businessUnit);
		//occObj.put("subBusinessUnit", subBusUnit);
		occObj.put("successFlag", successFlag);
		occObj.put("tag", tag);
		occObj.put("topology", topologyName);
		if(!topologyName.equalsIgnoreCase("Telluride"))
			occObj.put("purchaseOccasion", purOcca);
			
		BasicDBObject keyObj = new BasicDBObject();
		keyObj.append(MongoNameConstants.L_ID, l_id);
		keyObj.append(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		
		occasionResonsesCollection.update(keyObj, occObj, true, false);
	
	}*/
	
}
