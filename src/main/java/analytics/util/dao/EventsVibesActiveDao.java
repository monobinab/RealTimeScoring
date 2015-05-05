package analytics.util.dao;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

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
		//HashMap<String, String> buCustEventsMap = null;
		
		while (dbCursor.hasNext()) {
			record = dbCursor.next();
			if(record!=null){
				
				if(activeEventsMap.get((String)record.get(MongoNameConstants.PURCHASE_OCCASSION))!=null){
					HashMap<String, String> map = activeEventsMap.get((String)record.get(MongoNameConstants.PURCHASE_OCCASSION));
					map.put((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT), (String)record.get(MongoNameConstants.CUST_VIBES_EVENT));
				}
				else{	
					HashMap<String, String> map = new HashMap<String, String>(); 
					map.put((String)record.get(MongoNameConstants.ACTIVE_BUSINESS_UNIT), (String)record.get(MongoNameConstants.CUST_VIBES_EVENT));
					activeEventsMap.put((String)record.get(MongoNameConstants.PURCHASE_OCCASSION), map);
				}
			}
		}
		
		System.out.println(activeEventsMap);
		return activeEventsMap;
	}
	
	
}

