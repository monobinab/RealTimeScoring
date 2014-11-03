package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class SourcesDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SourcesDao.class);
    DBCollection sources;
    public SourcesDao(){
    	super();
    	sources = db.getCollection("sources");
    }

	public Map<String,String> getSources(){
		Map<String,String> sourcesMap = new HashMap<String, String>();
		BasicDBObject query = new BasicDBObject();
		DBCursor cursor = sources.find(query);
		for(DBObject obj:cursor){
			sourcesMap.put((String)obj.get(MongoNameConstants.SOURCES_S), (String)obj.get(MongoNameConstants.SOURCES_N));
		}
		return sourcesMap;
	}
}
