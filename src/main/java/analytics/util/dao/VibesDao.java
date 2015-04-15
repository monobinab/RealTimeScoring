package analytics.util.dao;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.Constants;
import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class VibesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory.getLogger(VibesDao.class);
	DBCollection vibesCollection;

	public VibesDao() {
		super();
		vibesCollection = db.getCollection("vibes");
		LOGGER.info("colelction in tagMetadataDao: " + vibesCollection.getFullName());

	}

	public List<DBObject> getVibes(String fetchInd) {

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

	}

}
