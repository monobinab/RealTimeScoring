package analytics.util.dao;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import analytics.util.objects.ClientAPIKey;

public class ClientApiKeysDAO extends AbstractDao {
	
	  DBCollection clientApiKeysCollection;
	    public ClientApiKeysDAO(){
	    	super();
	    	if(db != null){
	    		clientApiKeysCollection = db.getCollection("clientApiKeys");
	    	}
	    }
	
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
	
	public String getClientKey(String param){
		DBObject query = new BasicDBObject("clientName", param);
		DBObject doc=clientApiKeysCollection.findOne(query);
		return (String) doc.get("clientKey");
	}
	
}
