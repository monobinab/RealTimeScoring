package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
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
		Map<String, List<String>> feedBoostsMap = new HashMap<String, List<String>>();
		DBCursor vCursor = feedBoostsCollection.find();
		for (DBObject fd : vCursor) {
			BasicDBList boosts = (BasicDBList) fd.get(MongoNameConstants.BOOSTS_ARRAY);
			for(String b: boosts.keySet()) {
				if(!feedBoostsMap.containsKey(fd)) {
					feedBoostsMap.put(fd.get("topic").toString(), new ArrayList<String>());
				}
				feedBoostsMap.get(fd).add(b);
			}
		}
		return feedBoostsMap.get(feed);
	}
}
