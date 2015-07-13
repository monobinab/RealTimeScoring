package analytics.util.dao;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class VibesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(VibesDao.class);
	DBCollection vibesCollection;

	public VibesDao() {
		super();
		vibesCollection = db.getCollection("vibesResponses");
		LOGGER.info("colelction in tagMetadataDao: " + vibesCollection.getFullName());

	}

	/*public List<DBObject> getVibes(String fetchInd) {

		List<DBObject> vibesList = null;
		DBCursor cursor = vibesCollection.find(
				new BasicDBObject(MongoNameConstants.PROCESSED_FLAG, fetchInd))
				.limit(500);

		vibesList = cursor.toArray();

		return vibesList;
	}

	public void updateVibes(String l_id) {

		DBObject processUpdate = new BasicDBObject();
		processUpdate.put(MongoNameConstants.L_ID, l_id);

		LOGGER.info("Updating processed Vibes Lid " + l_id + " in collection "
				+ vibesCollection.getDB().getName());
		vibesCollection.update(
				new BasicDBObject(MongoNameConstants.L_ID, l_id),
				new BasicDBObject(MongoNameConstants.PROCESSED_FLAG, Constants.YES), true, false);

	}*/
	
	public void addVibesResponse(String l_id, String occasion, String successFlag) {
		
		DBObject vibesObj = new BasicDBObject();
		vibesObj.put(MongoNameConstants.L_ID, l_id);
		
		Date dNow = new Date( );
		SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss.SSS");
		vibesObj.put(MongoNameConstants.TIMESTAMP, ft.format(dNow));
		vibesObj.put("occasion", occasion);
		vibesObj.put("successFlag", successFlag);
		
		vibesCollection.insert(vibesObj);
	
		LOGGER.info("PERSIST: " + ft.format(dNow) + ", Topology: Vibes, lid: " + l_id 
				+", successFlag: "+successFlag);
	
	}

}
