package analytics.util;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class HostPortUtility {
	private static final Logger LOGGER = LoggerFactory.getLogger(DBConnection.class);
	static Map<String, Object> connectionsMap = new HashMap<String, Object>();
	
	 public static Map<String, Object> getConnectionsMap() {
		return connectionsMap;
	}
	public static void setConnectionsMap(Map<String, Object> connectionsMap) {
		HostPortUtility.connectionsMap = connectionsMap;
	}
	static MongoClient mongoClient;
	@SuppressWarnings("unchecked")
	public static void getEnvironment(String nimbusHost){
		try {
			mongoClient = new MongoClient("trprmongod1.vm.itg.corp.us.shldcorp.com", 27000);
		} catch (UnknownHostException e) {
			LOGGER.error("Mongo host unknown",e);
		}
		DB db = mongoClient.getDB("test");
		db.authenticateCommand("appuser", "sears123".toCharArray());
		DBCollection prodQaColl = db.getCollection("stormProdQaUrls");
		DBCursor cursor = prodQaColl.find(new BasicDBObject());
		while(cursor.hasNext()){
			DBObject dbObj = cursor.next();
			for(String key:dbObj.keySet()){
				if(!key.equals("_id"))
					connectionsMap.put(key, dbObj.get(key));
			}
		}
		
		 Map<String, Object> hostPortMap = HostPortUtility.getConnectionsMap();
	     System.setProperty(MongoNameConstants.IS_PROD, (String) hostPortMap.get(nimbusHost));
		//connectionsMap = (Map<String, String>) obj;
		//setConnectionsMap((Map<String, Object>) obj);
		//return RedisConnection.getServers(getConnectionsMap().get(nimbusHost));
	}
	
}