package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class SourceFeedDao extends AbstractDao{

	private static final Logger LOGGER = LoggerFactory
			.getLogger(SourceFeedDao.class);
	DBCollection sourceFeedCollection;

	public SourceFeedDao() {
		sourceFeedCollection = db.getCollection("sourceFeed");
	}
	
	public Map<String, String> getSourceFeedMap(){
		Map<String, String> sourceFeedMap = new HashMap<String, String>();
		DBObject dbObj =  sourceFeedCollection.findOne();
		for(String key:dbObj.keySet()){
			sourceFeedMap.put(key, dbObj.get(key).toString());
		}
		
		return sourceFeedMap;
	}

}
