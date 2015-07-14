package analytics.util.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class ClientApiKeysDAO extends AbstractDao {
	
	
	public String findkey(String param)
	{
				
		DBCollection clientApiKeysCollection=db.getCollection("clientApiKeys");  
		
		DBObject query = new BasicDBObject(param, new BasicDBObject(
                "$exists", true));	
		DBObject doc=clientApiKeysCollection.findOne(query);
		//System.out.println("Value is "+doc.get(param));
		String key =(String) doc.get(param);
		LOGGER.info("Getting the client key as "+ key +" in ClientApiKeysDAO");
		return key;
		
	}
	
}
