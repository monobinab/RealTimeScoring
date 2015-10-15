package analytics.util.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.MongoNameConstants;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class DivLnBuSubBuDao extends AbstractDao{

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DivLnBuSubBuDao.class);

    DBCollection divLnBuSubBuCollection;
    
    public DivLnBuSubBuDao(){
    	super();
    	divLnBuSubBuCollection = db.getCollection("divLnBuSubBu");
    }
    
    public Map<String, String> getDvLnBuSubBu(){
    	Map<String, String> divLnBuSubBuMap = new HashMap<String, String>();
    	DBCursor cursor = divLnBuSubBuCollection.find();
    	while(cursor.hasNext()){
    		DBObject obj = cursor.next();
    		divLnBuSubBuMap.put(obj.get(MongoNameConstants.DIV_LN).toString(), obj.get(MongoNameConstants.BU_SUBBU).toString());
    	}
    	
    	return divLnBuSubBuMap;
    }
}
