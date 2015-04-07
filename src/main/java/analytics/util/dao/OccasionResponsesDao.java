package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class OccasionResponsesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(OccasionResponsesDao.class);
	DBCollection occasionResonsesCollection;

	public OccasionResponsesDao() {
		super();
		occasionResonsesCollection = db.getCollection("occasionResponses");
	}


	public void addOccasionResponse(String l_id, String eid, String custEvent, String purOcca, String businessUnit, String subBusUnit) {
		
		DBObject occObj = new BasicDBObject();
		occObj.put(MongoNameConstants.L_ID, l_id);
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
		occObj.put(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		occObj.put("eid", eid);
		occObj.put("custEvent", custEvent);
		occObj.put("purchaseOccasion", purOcca);
		occObj.put("businessUnit", businessUnit);
		occObj.put("subBusinessUnit", subBusUnit);
		
		BasicDBObject keyObj = new BasicDBObject();
		keyObj.append(MongoNameConstants.L_ID, l_id);
		keyObj.append(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		
		occasionResonsesCollection.update(keyObj, occObj, true, false);
	
	}
	
}