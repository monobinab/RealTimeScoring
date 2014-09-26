package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import analytics.util.MongoNameConstants;
import analytics.util.objects.Variable;

public class BoostDao extends AbstractDao {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(BoostDao.class);
    DBCollection feedBoostsCollection;
    
    public BoostDao(){
    	super();
    	feedBoostsCollection = db.getCollection(MongoNameConstants.FEED_TO_BOOST_COLLECTION);
    }
	public List<String> getBoosts(String feed) {
		List<String> boostList = new ArrayList<String>();
		DBCursor feedCursor = feedBoostsCollection.find(new BasicDBObject(MongoNameConstants.FB_FEED,feed));
		while(feedCursor.hasNext()){
			DBObject feedObject = feedCursor.next();
			BasicDBList boosts = (BasicDBList) feedObject.get(MongoNameConstants.FB_BOOSTS);
			for( Iterator<Object> it = boosts.iterator(); it.hasNext(); )
				boostList.add(it.next().toString());
		}
		return boostList;
	}
	
	public Map<String, List<String>> getBoostsMap(List<String> feeds) {
		Map<String,List<String>> boostMap = new HashMap<String, List<String>>();
		for(String feed:feeds){
			List<String> boostList = new ArrayList<String>();
			DBCursor feedCursor = feedBoostsCollection.find(new BasicDBObject(MongoNameConstants.FB_FEED,feed));
			while(feedCursor.hasNext()){
				DBObject feedObject = feedCursor.next();
				BasicDBList boosts = (BasicDBList) feedObject.get(MongoNameConstants.FB_BOOSTS);
				for( Iterator<Object> it = boosts.iterator(); it.hasNext(); )
					boostList.add(it.next().toString());
			}
			boostMap.put(feed, boostList);
		}
		return boostMap;
	}
}
