/**
 * 
 */
package analytics.util.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;
import analytics.util.objects.VariableModel;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * @author spannal
 *
 */
public class CpsOccasionsDao extends AbstractDao {
	
	private static final Logger LOGGER = LoggerFactory
			.getLogger(CpsOccasionsDao.class);
    DBCollection cpsOccasionsCollection;
    public CpsOccasionsDao(){
    	super();
    	cpsOccasionsCollection = db.getCollection("cpsOccasions");
    }
    
    public HashMap<String, String> getcpsOccasionDurations(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
	
		DBCursor cpsOccasionsCursor = cpsOccasionsCollection.find();
    	for(DBObject cpsOccasionsDBObject: cpsOccasionsCursor) {
    		
    		String occasion = cpsOccasionsDBObject.get(MongoNameConstants.OCCASIONID).toString();
    		String expDuration =  cpsOccasionsDBObject.get(MongoNameConstants.TAGEXPIRESIN).toString();
    		
    		cpsOccasionDurationsMap.put(occasion, expDuration);
    		
        }
    	return cpsOccasionDurationsMap;
    }
    
    public HashMap<String, String> getcpsOccasionPriority(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
	
		DBCursor cpsOccasionsCursor = cpsOccasionsCollection.find();
    	for(DBObject cpsOccasionsDBObject: cpsOccasionsCursor) {
    		
    		String priority = cpsOccasionsDBObject.get(MongoNameConstants.PRIORITY).toString();
    		String occasion =  cpsOccasionsDBObject.get(MongoNameConstants.OCCASIONID).toString();
    		
    		cpsOccasionDurationsMap.put(occasion, priority);
    		
        }
    	return cpsOccasionDurationsMap;
    }
    
    public HashMap<String, String> getcpsOccasionId(){
    	HashMap<String, String> cpsOccasionDurationsMap = new HashMap<String, String>();
	
		DBCursor cpsOccasionsCursor = cpsOccasionsCollection.find();
    	for(DBObject cpsOccasionsDBObject: cpsOccasionsCursor) {
    		
    		String occasionId = cpsOccasionsDBObject.get(MongoNameConstants.OCCASIONID).toString();
    		String occasion =  cpsOccasionsDBObject.get(MongoNameConstants.OCCASION).toString();
    		
    		cpsOccasionDurationsMap.put(occasion, occasionId);
    		
        }
    	return cpsOccasionDurationsMap;
    }
    
    public HashMap<String, String> getcpsOccasionsById(){
    	HashMap<String, String> cpsOccasionsMap = new HashMap<String, String>();
	
		DBCursor cpsOccasionsCursor = cpsOccasionsCollection.find();
    	for(DBObject cpsOccasionsDBObject: cpsOccasionsCursor) {
    		
    		String occasionId = cpsOccasionsDBObject.get(MongoNameConstants.OCCASIONID).toString();
    		String occasion =  cpsOccasionsDBObject.get(MongoNameConstants.OCCASION).toString();
    		
    		cpsOccasionsMap.put(occasionId,occasion);
    		
        }
    	return cpsOccasionsMap;
    }
     

}
