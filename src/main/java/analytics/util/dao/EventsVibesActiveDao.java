package analytics.util.dao;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;
import analytics.util.objects.TagMetadata;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class EventsVibesActiveDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(MemberTraitsDao.class);
	DBCollection eventsVibesActiveCollection;

	
	public EventsVibesActiveDao() {
		super();
		eventsVibesActiveCollection = db.getCollection("eventsVibesActive");
		LOGGER.info("collection in EventsVibesActiveDao: " + eventsVibesActiveCollection.getFullName());
	}

	public HashMap<String, HashMap<String, String>> getVibesActiveEventsList(){

		DBCursor dbCursor = eventsVibesActiveCollection.find();
		DBObject record = null;
		HashMap<String, HashMap<String, String>> activeEventsMap = new HashMap<String, HashMap<String, String>>();
		HashMap<String, String> buCustEventsMap = new HashMap<String, String>();
		
		while (dbCursor.hasNext()) {
			record = dbCursor.next();
			if(record!=null){
				buCustEventsMap.put((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT), (String)record.get(MongoNameConstants.CUST_VIBES_EVENT));
				activeEventsMap.put((String)record.get(MongoNameConstants.PURCHASE_OCCASSION), buCustEventsMap);
			}
		}
		return activeEventsMap;
	}
	
	
}

